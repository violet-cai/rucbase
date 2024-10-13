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

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        // 获取需要删除的表
        tab_ = sm_manager_->db_.get_table(tab_name);
        // 获取需要删除的表的数据文件句柄（RmFileHandle）
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        // @NOTE: 为了方便上手，我们将所有需要删除的record的rid初始化在了Delete算子的上下文信息中
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // !djb20241013 Todo:
        // 1. 遍历所有需要删除的record的rid
        // 2. 将指定rid的record通过RmFileHandle的delete_record函数从表数据文件中删除
        // 3. 如果表上存在索引，
        // 4. 将指定rid的record通过IxIndexHandle的delete_entry函数从索引文件中删除
        // lab4: 记录删除操作（for transaction rollback）

        // insert和delete操作不需要返回record对应指针，返回nullptr即可
        RmRecord rec(fh_->get_file_hdr().record_size);

        // 1. 将record对象通过RmFileHandle插入到对应表的数据文件中
        for (const auto &rid : rids_) {
            fh_->delete_record(rid, context_);
        }

        // 2. 如果表上存在索引，将record对象插入到相关索引文件中
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t i = 0; i < index.col_num; ++i) {
                memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
                offset += index.cols[i].len;
            }
            ih->delete_entry(key, context_->txn_);
        }

        // lab4: 记录插入操作（for transaction rollback）
        for (const auto &rid : rids_) {
            WriteRecord *write_rec = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid);
            context_->txn_->append_write_record(write_rec);
        }
        // insert和delete操作不需要返回record对应指针，返回nullptr即可
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};