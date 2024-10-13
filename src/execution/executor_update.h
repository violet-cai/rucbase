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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    std::unique_ptr<RmRecord> Next() override {
        // 类似于Delete，一方面更新fh_，update_record
        // 另一方面从每个索引里(检查是否由setClause,)delete then insert
        // 1. 从TabMeta tab_获取所有的索引列，从sm_manager_拿到所有的索引句柄
        int ih_num = tab_.indexes.size();
        std::vector<IxIndexHandle*> ihs(ih_num);
        for(int i=0;i<ih_num;i++){
            ihs[i] = (sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_,tab_.indexes[i].cols))).get();
        }
        // 2. 遍历rid
        int rid_num = rids_.size();
        for(int i=0;i<rid_num;i++){
            auto rec = fh_->get_record(rids_[i],context_);
            RmRecord updated_rec = RmRecord(rec->size);         // lab4
            memcpy(updated_rec.data,rec->data,rec->size);       // lab4
            // 2.1 算新的data
            int set_num = set_clauses_.size();
            char* new_data = new char[rec->size+1];
            memset(new_data,0,rec->size+1);
            memcpy(new_data,rec->data,rec->size+1);
            for(int k=0;k<set_num;k++){
                std::string cur_col = set_clauses_[k].lhs.col_name;
                auto col_meta_ptr = tab_.get_col(cur_col);
                memcpy(new_data+col_meta_ptr->offset,set_clauses_[k].rhs.raw->data,col_meta_ptr->len);
            }
            // 2.2 更新索引项
            for(int j=0;j<ih_num;j++){
                IndexMeta index_meta = tab_.indexes[j];
                // 按顺序拼接多级索引各个列的值，得到key
                char* key = new char[index_meta.col_tot_len+1];
                char* newkey = new char[index_meta.col_tot_len+1];
                key[0] = '\0';
                newkey[0] = '\0';
                int curlen = 0;
                for(int k=0;k<index_meta.col_num;k++){
                    // strncat(key,rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    memcpy(key+curlen,rec->data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    // strncat(newkey,new_data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    memcpy(newkey+curlen,new_data+index_meta.cols[k].offset,index_meta.cols[k].len);
                    curlen += index_meta.cols[k].len;
                    key[curlen] = '\0';
                    newkey[curlen] = '\0';
                }
                // 调用delete_entry删除该key
                ihs[j]->delete_entry(key,context_->txn_);
                ihs[j]->insert_entry(newkey,rids_[i],context_->txn_);
            }
            // 2.3 写新data
            // TODO 这里锁可能也有点问题
            fh_->update_record(rids_[i],new_data,context_);

            // lab4 modify write_set
            WriteRecord* write_rec = new WriteRecord(WType::UPDATE_TUPLE,tab_name_,rids_[i],updated_rec);
            context_->txn_->append_write_record(write_rec);
        }
        // TODO: return what
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};