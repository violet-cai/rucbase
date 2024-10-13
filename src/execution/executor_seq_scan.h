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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;                           // me:当前指向的
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    size_t tupleLen() const { return len_; };

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // 初始化的时候scan会指向第一个有值的
        scan_ = std::make_unique<RmScan>(fh_);
        // 手动让scan指向第一个符合条件的
        if(scan_->is_end()){
            return;
        }
        // TODO: 如果表是空着的，外层直接Next()可能会出事
        for(;!scan_->is_end();scan_->next()){
            rid_ = scan_->rid();
            auto cur_record_ptr = fh_->get_record(rid_,context_);
            if(check_conds(cur_record_ptr.get())){
                break;
            }
        }
        return;
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        if(scan_->is_end()){
            return;
        }
        for(scan_->next();!scan_->is_end();scan_->next()){
            rid_ = scan_->rid();
            // TODO: if table is empty, means rid_ = {0,-1};
            auto cur_record_ptr = fh_->get_record(rid_,context_);
            if(check_conds(cur_record_ptr.get())){
                break;
            }
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        // 这个应该是上一层获取record的接口
        // 规定上一层通过nextTuple进行rid修改，再通过Next()获取rm
        // 因此在这个函数里不再调用nextTuple
        // nextTuple();
        return fh_->get_record(rid_,context_);
    }

    Rid &rid() override { return rid_; }

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