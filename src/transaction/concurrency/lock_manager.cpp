/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 0. 上锁
    std::unique_lock<std::mutex> lock{latch_};
    // 1. 检查并更新事务状态 2PL
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        // TODO how to handle this
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2. 检查当前事务是否已经上过该记录的锁，行锁不含意向锁，所以对于读锁来说上过锁（S or X）就可以返回了
    LockDataId rec_lockID = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto txn_locks = txn->get_lock_set();
    if (txn_locks->find(rec_lockID) != txn_locks->end()) {
        return true;
    }
    // 3. 检查该行上是否有其它事务的写锁，如果有，本事务需要abort no-wait
    for (auto lock_it = lock_table_[rec_lockID].request_queue_.begin();
         lock_it != lock_table_[rec_lockID].request_queue_.end(); ++lock_it) {
        if (lock_it->txn_id_ != txn->get_transaction_id() && lock_it->lock_mode_ == LockMode::EXLUCSIVE) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        }
    }
    // 4. 2&3检查通过，开始颁发锁
    // 4.1 把该锁insert进本事务的锁集
    txn_locks->insert(rec_lockID);
    // 4.2 创建新的锁申请，全局锁，更新group_lock_mode_
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
    }
    return true;
    // TODO 阻塞？
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // TODO 2.:
    // 0. 对锁表加latch
    std::unique_lock<std::mutex> lock{latch_};
    // 1. 检查并更新事务状态是否遵循两阶段封锁原则
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        // TODO how to handle this
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2. 检查锁表中的锁是否与当前申请的写锁冲突
    LockDataId rec_lockID = LockDataId(tab_fd, rid, LockDataType::RECORD);
    std::list<LockRequest>::iterator lock_it;
    bool lock_found = false;
    for (auto it = lock_table_[rec_lockID].request_queue_.begin(); it != lock_table_[rec_lockID].request_queue_.end();
         ++it) {
        if (it->txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        } else {
            if (it->lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已有x
            } else {
                lock_it = it;
                lock_found = true;
            }
        }
    }
    auto txn_locks = txn->get_lock_set();
    // if (txn_locks->find(rec_lockID) != txn_locks->end()) {
    //     return true;
    // }
    // 3. 检查本事务是否已经持有当前记录的锁，有读锁则升级成写锁
    if (lock_found) {
        lock_it->lock_mode_ = LockMode::EXLUCSIVE;
        if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X) {
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
        }
        return true;
    }
    // 4. 没有其它锁，也没有自身的锁，颁发新的锁
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
    }
    return true;
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    // 0.
    std::unique_lock<std::mutex> lock{latch_};
    // 1.
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        // TODO how to handle this
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2 检查是否有其它事务的X IX SIX锁，如果有则abort
    LockDataId rec_lockID = LockDataId(tab_fd, LockDataType::TABLE);
    bool stronger_found = false;             // for S X SIX
    std::list<LockRequest>::iterator is_it;  // for IS should update to S
    bool is_found = false;
    std::list<LockRequest>::iterator ix_it;  // for IX should update to SIX
    bool ix_found = false;
    for (auto it = lock_table_[rec_lockID].request_queue_.begin(); it != lock_table_[rec_lockID].request_queue_.end();
         it++) {
        if (it->txn_id_ != txn->get_transaction_id()) {
            if (it->lock_mode_ == LockMode::EXLUCSIVE || it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
                it->lock_mode_ == LockMode::S_IX) {
                throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                return false;
            }
        } else {
            // TODO granted?
            if (it->lock_mode_ == LockMode::INTENTION_SHARED) {
                is_it = it;
                is_found = true;
            } else if (it->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                ix_it = it;
                ix_found = true;
            } else {
                stronger_found = true;
            }
        }
    }
    // 3 检查当前事务是否有其它锁 如果已经有了S、X、SIX锁，直接返回； 如果IS，升级成S锁
    //      如果当前事务已经持有了IX，升级成SIX
    //      理论上一个事务应该只会有一个锁
    if (stronger_found) {
        return true;
    }
    if (is_found) {
        // have a IS lock -> S lock
        is_it->lock_mode_ = LockMode::SHARED;
        if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S) {
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
        }
        return true;
    }
    if (ix_found) {
        // have a IX lock -> SIX lock
        ix_it->lock_mode_ = LockMode::S_IX;
        if (lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X) {
            // SIX以上的只有X，这里发现enum没按偏序顺序来
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::SIX;
        }
        return true;
    }
    // 4. 颁发新的锁
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::SHARED);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::S) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::S;
    }
    return true;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    // 0
    std::unique_lock<std::mutex> lock{latch_};
    // 1
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2 检查是否有其他事务的锁，任何一种都需要abort
    LockDataId rec_lockID = LockDataId(tab_fd, LockDataType::TABLE);
    bool weaker_found = false;
    std::list<LockRequest>::iterator weaker_it;
    bool x_found = false;
    for (auto it = lock_table_[rec_lockID].request_queue_.begin(); it != lock_table_[rec_lockID].request_queue_.end();
         it++) {
        if (it->txn_id_ != txn->get_transaction_id()) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        } else {
            if (it->lock_mode_ == LockMode::EXLUCSIVE) {
                x_found = true;
            } else {
                weaker_found = true;
                weaker_it = it;
            }
        }
    }
    // 3 检查当前事务是否有其它锁
    if (x_found) {
        return true;
    }
    if (weaker_found) {
        weaker_it->lock_mode_ = LockMode::EXLUCSIVE;
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
        return true;
    }
    // 4
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::EXLUCSIVE);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::X;
    }
    return true;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    // 0
    std::unique_lock<std::mutex> lock{latch_};
    // 1
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2 检查是否有其它事务的锁，X锁需要回滚
    LockDataId rec_lockID = LockDataId(tab_fd, LockDataType::TABLE);
    bool have_lock = false;
    for (auto it = lock_table_[rec_lockID].request_queue_.begin(); it != lock_table_[rec_lockID].request_queue_.end();
         it++) {
        if (it->txn_id_ != txn->get_transaction_id() && it->lock_mode_ == LockMode::EXLUCSIVE) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        } else {
            have_lock = true;
        }
    }
    // 3 检查当前事务的锁，不需要升级，任何一种锁都可以return true
    if (have_lock) {
        return true;
    }
    // 4
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::INTENTION_SHARED);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::IS) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IS;
    }
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    // 0
    std::unique_lock<std::mutex> lock{latch_};
    // 1
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::SHRINKING) {
        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        return false;
    } else if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        return false;
    }
    txn->set_state(TransactionState::GROWING);
    // 2 检查是否有其它事务的锁，S X SIX需要abort
    LockDataId rec_lockID = LockDataId(tab_fd, LockDataType::TABLE);
    bool s_found = false;  // for s upgrade to SIX
    std::list<LockRequest>::iterator s_it;
    bool is_found = false;  // for is upgrade to IX
    std::list<LockRequest>::iterator is_it;
    bool have_stronger = false;  // for X SIX IX
    for (auto it = lock_table_[rec_lockID].request_queue_.begin(); it != lock_table_[rec_lockID].request_queue_.end();
         it++) {
        if (it->txn_id_ != txn->get_transaction_id() &&
            (it->lock_mode_ == LockMode::SHARED || it->lock_mode_ == LockMode::EXLUCSIVE ||
             it->lock_mode_ == LockMode::S_IX)) {
            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
            return false;
        } else {
            if (it->lock_mode_ == LockMode::SHARED) {
                s_found = true;
                s_it = it;
            } else if (it->lock_mode_ == LockMode::INTENTION_SHARED) {
                is_found = true;
                is_it = it;
            } else {
                have_stronger = true;
            }
        }
    }
    // 3 检查是否有当前事务的锁，S锁需要升级成SIX，X\IX\SIX锁return true，IS升级成IX
    if (have_stronger) {
        return true;
    }
    if (s_found) {
        // S -> SIX
        s_it->lock_mode_ = LockMode::S_IX;
        if (lock_table_[rec_lockID].group_lock_mode_ != GroupLockMode::X) {
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::SIX;
        }
        return true;
    }
    if (is_found) {
        // IS -> IX
        is_it->lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
        if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X) {  // 这个条件排除了X和SIX enum没有按顺序
            lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IX;
        }
        return true;
    }
    // 4
    auto txn_locks = txn->get_lock_set();
    txn_locks->insert(rec_lockID);
    LockRequest lock_request = LockRequest(txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE);
    lock_request.granted_ = true;
    lock_table_[rec_lockID].request_queue_.push_back(lock_request);
    if (lock_table_[rec_lockID].group_lock_mode_ < GroupLockMode::X) {
        lock_table_[rec_lockID].group_lock_mode_ = GroupLockMode::IX;
    }
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    // 0 上锁
    std::unique_lock<std::mutex> lock{latch_};
    // 1 检查并修改事务状态为shrinking 2PL
    TransactionState txn_stat = txn->get_state();
    if (txn_stat == TransactionState::ABORTED || txn_stat == TransactionState::COMMITTED) {
        // TODO how to handle this
        return false;
    }
    txn->set_state(TransactionState::SHRINKING);
    // 2 检查txn和data_id是否匹配
    if (txn->get_lock_set()->find(lock_data_id) == txn->get_lock_set()->end()) {
        return false;
    }
    // 3 在全局锁表里删掉txn开的所有锁
    for (auto it = lock_table_[lock_data_id].request_queue_.begin();
         it != lock_table_[lock_data_id].request_queue_.end();) {
        if (it->txn_id_ == txn->get_transaction_id()) {
            it = lock_table_[lock_data_id].request_queue_.erase(it);
        } else {
            it++;
        }
    }
    // 4 修改lock_table_[lock_data_id]的GroupLockMode
    GroupLockMode new_mode = GroupLockMode::NON_LOCK;
    for (auto it = lock_table_[lock_data_id].request_queue_.begin();
         it != lock_table_[lock_data_id].request_queue_.end(); it++) {
        if (it->granted_) {
            switch (it->lock_mode_) {
                case LockMode::INTENTION_SHARED: {
                    if (new_mode == GroupLockMode::NON_LOCK) {
                        new_mode = GroupLockMode::IS;
                    }
                    break;
                }
                case LockMode::INTENTION_EXCLUSIVE: {
                    if (new_mode == GroupLockMode::NON_LOCK || new_mode == GroupLockMode::IS) {
                        new_mode = GroupLockMode::IX;
                    } else if (new_mode == GroupLockMode::S) {
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
                case LockMode::SHARED: {
                    if (new_mode == GroupLockMode::NON_LOCK || new_mode == GroupLockMode::IS) {
                        new_mode = GroupLockMode::S;
                    } else if (new_mode == GroupLockMode::IX) {
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
                case LockMode::EXLUCSIVE: {
                    new_mode = GroupLockMode::X;
                    break;
                }
                case LockMode::S_IX: {
                    if (new_mode != GroupLockMode::X) {
                        new_mode = GroupLockMode::SIX;
                    }
                    break;
                }
            }
        }
    }
    lock_table_[lock_data_id].group_lock_mode_ = new_mode;
    return true;
}
