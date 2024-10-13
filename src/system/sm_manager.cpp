/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */
// #define DEBUG
#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"

/**
 * @description: 判断是否为一个文件夹
 * @return {bool} 返回是否为一个文件夹
 * @param {string&} db_name 数据库文件名称，与文件夹同名
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: 创建数据库，所有的数据库相关文件都放在数据库同名文件夹下
 * @param {string&} db_name 数据库名称
 */
void SmManager::create_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        throw DatabaseExistsError(db_name);
    }
    // 为数据库创建一个子目录
    std::string cmd = "mkdir " + db_name;
    if (system(cmd.c_str()) < 0) {  // 创建一个名为db_name的目录
        throw UnixError();
    }
    if (chdir(db_name.c_str()) < 0) {  // 进入名为db_name的目录
        throw UnixError();
    }
    // 创建系统目录
    DbMeta* new_db = new DbMeta();
    new_db->name_ = db_name;

    // 注意，此处ofstream会在当前目录创建(如果没有此文件先创建)和打开一个名为DB_META_NAME的文件
    std::ofstream ofs(DB_META_NAME);

    // 将new_db中的信息，按照定义好的operator<<操作符，写入到ofs打开的DB_META_NAME文件中
    ofs << *new_db;  // 注意：此处重载了操作符<<

    delete new_db;

    // 创建日志文件
    disk_manager_->create_file(LOG_FILE_NAME);

    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 删除数据库，同时需要清空相关文件以及数据库同名文件夹
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::drop_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    std::ifstream ofs(DB_META_NAME);
    ofs >> db_;
    for (auto table = db_.tabs_.begin(); table != db_.tabs_.end(); table++) {
        std::string tab_name = table->first;
        fhs_.erase(table->first);

        TabMeta tab_meta = table->second;
        for (auto index : tab_meta.indexes) {
            ihs_.erase(table->first);
        }
    }
    std::string cmd = "rm -r \"" + db_name + "\"";
    if (system(cmd.c_str()) < 0) {
        throw UnixError();
    }
}

/**
 * @description: 打开数据库，找到数据库对应的文件夹，并加载数据库元数据和相关文件
 * @param {string&} db_name 数据库名称，与文件夹同名
 */
void SmManager::open_db(const std::string& db_name) {
    // 1. 判断数据库是否存在，若不存在抛出错误
    if (!is_dir(db_name)) {
        throw DatabaseNotFoundError(db_name);
    }
    // 2. 进入数据库文件夹，加载元数据
    if (chdir(db_name.c_str()) < 0) {
        throw UnixError();
    }
    std::ifstream ofs(DB_META_NAME);
    ofs >> db_;
    // 3. 加载fhs_和ihs_
    for (auto table = db_.tabs_.begin(); table != db_.tabs_.end(); table++) {
        std::string tab_name = table->first;
        // auto table_hdr_ptr = rm_manager_->open_file(tab_name);
        fhs_.emplace(table->first, rm_manager_->open_file(tab_name));

        TabMeta tab_meta = table->second;
        for (auto index : tab_meta.indexes) {
            // auto index_hdr_ptr =  ix_manager_->open_index(table->first,index.cols);
            ihs_.emplace(table->first, ix_manager_->open_index(table->first, index.cols));
        }
    }
}

/**
 * @description: 把数据库相关的元数据刷入磁盘中
 */
void SmManager::flush_meta() {
    // 默认清空文件
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: 关闭数据库并把数据落盘
 */
void SmManager::close_db() {
    // 1. 元数据信息落盘
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
    // 2. 关闭所有文件，close_file里实现落盘
    for (auto it = fhs_.begin(); it != fhs_.end(); it++) {
        rm_manager_->close_file(it->second.get());
    }
    for (auto it = ihs_.begin(); it != ihs_.end(); it++) {
        ix_manager_->close_index(it->second.get());
    }
    // 3. 清空ihs_,fhs_
    ihs_.clear();
    fhs_.clear();
    // 4. 清空元数据db_
    db_.name_ = "";
    db_.tabs_.clear();
    // 回到根目录
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: 显示所有的表,通过测试需要将其结果写入到output.txt,详情看题目文档
 * @param {Context*} context
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto entry = db_.tabs_.begin(); entry != db_.tabs_.end(); entry++) {
        auto& tab = entry->second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: 显示表的元数据
 * @param {string&} tab_name 表名称
 * @param {Context*} context
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta& tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto& col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: 创建表
 * @param {string&} tab_name 表的名称
 * @param {vector<ColDef>&} col_defs 表的字段
 * @param {Context*} context
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (db_.is_table(tab_name)) {
        throw TableExistsError(tab_name);
    }
    // Create table meta
    int curr_offset = 0;
    TabMeta tab;
    tab.name = tab_name;
    for (auto& col_def : col_defs) {
        ColMeta col = {.tab_name = tab_name,
                       .name = col_def.name,
                       .type = col_def.type,
                       .len = col_def.len,
                       .offset = curr_offset,
                       .index = false};
        curr_offset += col_def.len;
        tab.cols.push_back(col);
    }
    // Create & open record file
    int record_size = curr_offset;  // record_size就是col meta所占的大小（表的元数据也是以记录的形式进行存储的）
    rm_manager_->create_file(tab_name, record_size);
    db_.tabs_[tab_name] = tab;
    // fhs_[tab_name] = rm_manager_->open_file(tab_name);
    fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

    flush_meta();
    // TODO 加锁?
}

/**
 * @description: 删除表
 * @param {string&} tab_name 表的名称
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    // 删除相关的数据文件，更新元数据信息
    //  1. 检查表是否存在
    if (!db_.is_table(tab_name)) {
        throw TableNotFoundError(tab_name);
    }
    // 1.5 加锁
    context->lock_mgr_->lock_exclusive_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // 2. drop_index删除相关索引文件
    TabMeta& obj_table = db_.get_table(tab_name);
    auto table_hdr_ptr = fhs_[tab_name].get();
    for (auto index : obj_table.indexes) {
        drop_index(tab_name, index.cols, context);
    }
    // 3. rm_manager删除表文件
    rm_manager_->close_file(table_hdr_ptr);
    rm_manager_->destroy_file(tab_name);
    // 4. db_.tabs_更新，fhs_更新
    db_.tabs_.erase(tab_name);
    fhs_.erase(tab_name);
}

/**
 * @description: 创建索引
 * @param {string&} tab_name 表的名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 加锁 TODO 应该加IX吗
    // context->lock_mgr_->lock_IX_on_table(context->txn_,fhs_[tab_name]->GetFd());
    TabMeta& table = db_.get_table(tab_name);
    context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // TabMeta& table = db_.get_table(tab_name);
    if (table.is_index(col_names)) {
        throw IndexExistsError(tab_name, col_names);
    }

    IndexMeta index;
    int col_num = col_names.size();
    std::vector<ColMeta> cols;
    int tot_len = 0;
    for (int i = 0; i < col_num; i++) {
        auto cur_col_it = table.get_col(col_names[i]);
        cols.push_back(*cur_col_it);
        tot_len += cur_col_it->len;
    }
    index.tab_name = tab_name;
    index.col_tot_len = tot_len;
    index.col_num = col_num;
    index.cols = cols;

    table.indexes.push_back(index);

    // create b+ tree file?
    ix_manager_->create_index(tab_name, cols);

    // insert kv pairs into index_hdr
    auto index_hdr = ix_manager_->open_index(tab_name, cols);

    // get the records from table(rm handle)
    RmFileHandle* rm_hdr = fhs_.at(tab_name).get();
    // use rm_scan to traverse the table
    auto scan = RmScan(rm_hdr);
    while (!scan.is_end()) {
        Rid rid = scan.rid();
        // if the table is empty
        if (rid.slot_no < 0 && rid.page_no == 0) break;
        auto record = rm_hdr->get_record(rid, context);  // record: RmRecord*
        // record.data是所有col连续存储，要用偏移值找
        char* key = new char[tot_len + 1];
        key[0] = '\0';
        int curlen = 0;
        for (int i = 0; i < col_num; i++) {
            // 按顺序拼接每一个col的值
            strncat(key, record->data + cols[i].offset, cols[i].len);
            curlen += cols[i].len;
            key[curlen] = '\0';
        }
        index_hdr->insert_entry(key, rid, context->txn_);
        scan.next();
    }

    // insert the index_hdr into ihs_
    // ix_manager_->close_index(index_hdr.get());  //std::move會修改index_hdr的值
    ihs_.emplace(ix_manager_->get_index_name(tab_name, cols), std::move(index_hdr));
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<string>&} col_names 索引包含的字段名称
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    // 0. 加锁 TODO 应该加IX吗
    // context->lock_mgr_->lock_exclusive_on_table(context->txn_,fhs_[tab_name]->GetFd());
    // context->lock_mgr_->lock_IX_on_table(context->txn_,fhs_[tab_name]->GetFd());
    TabMeta& table = db_.get_table(tab_name);
    context->lock_mgr_->lock_shared_on_table(context->txn_, fhs_[tab_name]->GetFd());
    // 1. 拿到对应表，判断索引是否存在
    if (!table.is_index(col_names)) {
        throw IndexNotFoundError(tab_name, col_names);
    }
    // 2. ix_manager_删索引文件
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    ix_manager_->close_index(ihs_.at(index_name).get());
    ix_manager_->destroy_index(tab_name, col_names);
    // 3. 更新ihs_
    ihs_.erase(index_name);
    // 4. 更新table
    table.indexes.erase(table.get_index_meta(col_names));
}

/**
 * @description: 删除索引
 * @param {string&} tab_name 表名称
 * @param {vector<ColMeta>&} 索引包含的字段元数据
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    std::vector<std::string> col_names;
    std::string table_name = cols[0].tab_name;
    for (auto meta : cols) {
        col_names.push_back(meta.name);
    }
    drop_index(table_name, col_names, context);
}
