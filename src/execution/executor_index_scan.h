/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }
    
    // index_scan和seq_scan在这里的逻辑应该是一样的
    // 二者的主要区别应该在ix_scan和rm_scan里next的实现上
    // 外部接口的差别在于scan_的初始化上
    void beginTuple() override {
        auto ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,index_col_names_)).get();
        // find lower & upper for ixscan, using ix_manager's lower & upper
        // lower iid指向第一个有效的rid
        // upper end指向末尾
        Iid lower = ih_->leaf_begin();
        Iid upper = ih_->leaf_end();
        scan_ = std::make_unique<IxScan>(ih_,lower,upper,sm_manager_->get_bpm());
        if(scan_->is_end()){
            return ;
        }
        for(;!scan_->is_end();scan_->next()){
            rid_ = scan_->rid();
            auto cur_record_ptr = fh_->get_record(rid_,context_);
            if(check_conds(cur_record_ptr.get())){
                break;
            }
        }
        return;
    }

    void nextTuple() override {
        if(scan_->is_end()){
            return;
        }
        for(scan_->next();!scan_->is_end();scan_->next()){
            rid_ = scan_->rid();
            auto cur_record_ptr = fh_->get_record(rid_,context_);
            if(check_conds(cur_record_ptr.get())){
                break;
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_,context_);
    }

    Rid &rid() override { return rid_; }

    size_t tupleLen() const {return len_;}

    bool check_conds(RmRecord* record_ptr){
        int len = fed_conds_.size();
        if(len == 0)
            return true;
        bool found = check_cond(fed_conds_[0],record_ptr);
        for(int i=1;i<len;i++){
            found &= check_cond(fed_conds_[i],record_ptr);
            if(!found)
                return false;
        }
        return found;
    }

    bool check_cond(Condition cond_, RmRecord* cur_record_ptr){
        // 1. 获取被比较的左列值
        auto left_col_it = get_col(cols_,cond_.lhs_col);
        char* left_val = cur_record_ptr->data + left_col_it->offset;
        int len = left_col_it->len;
        // 2. 检查右列是否是常值，并获取相应的列值/常值
        char* right_val;
        ColType col_type;
        if(cond_.is_rhs_val){
            // 常值
            Value right_value = cond_.rhs_val;
            right_val = right_value.raw->data;
            col_type = right_value.type;
        }
        else{
            // 同左值
            auto right_col_it = get_col(cols_,cond_.rhs_col);
            right_val = cur_record_ptr->data + right_col_it->offset;
            col_type = right_col_it->type;
        }
        // 3. 根据比较条件判断true false
        int cmp = ix_compare(left_val,right_val,col_type,len);
        bool found;
        switch(cond_.op){
            // OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE
            case OP_EQ:
            {
                found = (cmp==0);
                break;
            }
            case OP_NE:
            {
                found = (cmp!=0);
                break;
            }
            case OP_LT:{
                found = (cmp==-1);
                break;
            }
            case OP_GT:{
                found = (cmp==1);
                break;
            }
            case OP_LE:{
                found = (cmp!=1);
                break;
            }
            case OP_GE:{
                found = (cmp!=-1);
                break;
            }
        }
        return found;
    }

    const std::vector<ColMeta> &cols() const {
        // std::vector<ColMeta> *_cols = nullptr;
        return cols_;
    };

    bool is_end() const { return scan_->is_end(); };
};