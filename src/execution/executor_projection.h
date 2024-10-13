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

class ProjectionExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段 me:投影结束以后的colMeta串
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  // me: 被选择的列在提取前的index（第几列）

   public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols) {
        prev_ = std::move(prev);

        size_t curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col : sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
    }

    size_t tupleLen() const { return len_; };

    void beginTuple() override {
        // 递归begin孩子
        prev_->beginTuple();
    }       

    void nextTuple() override {
        // 目的：调用儿子节点的next
        prev_->nextTuple();
    }

    std::unique_ptr<RmRecord> Next() override {
        // 提取出对应列的数据，组装成一条新的记录，返回给上层
        auto child_rec = prev_->Next();
        auto ret_rec = std::make_unique<RmRecord>(len_);
        int sel_num = sel_idxs_.size();
        auto prev_cols = prev_->cols();

        for(int i=0;i<sel_num;i++){
            // 对于cols_里的每一列，需要找到它在child_rec里的offset、len，以及它未来在ret_rec里的offset，并使用memcpy进行复制
            ColMeta prev_col = prev_cols[sel_idxs_[i]];
            ColMeta cur_col = cols_[i];
            char* prev_val = child_rec->data+prev_col.offset;
            char* cur_val = ret_rec->data + cur_col.offset;
            int len = prev_col.len;
            memcpy(cur_val,prev_val,len);
        }
        return ret_rec;
    }

    Rid &rid() override { return _abstract_rid; }

    const std::vector<ColMeta> &cols() const {
        // std::vector<ColMeta> *_cols = nullptr;
        return cols_;
    };

    bool is_end() const { return prev_->is_end(); };
};