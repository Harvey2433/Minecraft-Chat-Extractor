import os
import threading
import queue
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime
import re
import chardet
import time
import traceback
import gzip
import shutil
import tempfile
import psutil
import math
import logging
from logging.handlers import RotatingFileHandler
import ctypes
import sys
import hashlib


# 配置日志系统
def setup_logging():
    # 创建日志目录
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 创建日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 创建文件处理器 - 最多保留5个日志文件，每个最大10MB
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, "chat_extractor.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 创建日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器到记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# 初始化日志
logger = setup_logging()


# 设置文件隐藏属性
def set_hidden(path):
    if os.name == 'nt':  # Windows
        try:
            ctypes.windll.kernel32.SetFileAttributesW(path, 2)
        except:
            pass
    else:  # Unix/Linux/Mac
        if not path.startswith('.'):
            hidden_path = os.path.join(os.path.dirname(path), '.' + os.path.basename(path))
            os.rename(path, hidden_path)
            return hidden_path
    return path


class LogProcessor:
    def __init__(self):
        # 使用列表形式的前缀匹配
        self.prefixes = [
            "[Server thread/INFO] [net.minecraft.server.MinecraftServer/]:",
            "[Server thread/INFO] [Console/]:"
        ]
        # 扩展备选编码列表
        self.backup_encodings = [
            'utf-8', 'gbk', 'gb2312', 'gb18030', 'big5',
            'latin1', 'cp1252', 'utf-16', 'utf-16-le', 'utf-16-be',
            'iso-8859-1', 'iso-8859-15', 'euc-kr', 'shift_jis',
            'cp932', 'cp936', 'cp949', 'cp950', 'ascii'
        ]
        self.stop_requested = False

    def set_stop_requested(self, stop):
        self.stop_requested = stop

    def count_lines(self, file_path):
        """快速估算文件行数"""
        try:
            with open(file_path, 'rb') as f:
                lines = 0
                buf_size = 1024 * 1024  # 1MB缓冲区
                read_f = f.raw.read if hasattr(f, 'raw') else f.read

                buf = read_f(buf_size)
                while buf:
                    lines += buf.count(b'\n')
                    if self.stop_requested:
                        return lines
                    buf = read_f(buf_size)
                return lines
        except Exception as e:
            logger.error(f"行数统计失败: {str(e)}")
            return 0

    def detect_encoding(self, file_path):
        """增强的文件编码检测方法"""
        # 1. 首先检查BOM头
        bom_encodings = {
            b'\xef\xbb\xbf': 'utf-8',
            b'\xff\xfe': 'utf-16-le',
            b'\xfe\xff': 'utf-16-be',
            b'\xff\xfe\x00\x00': 'utf-32-le',
            b'\x00\x00\xfe\xff': 'utf-32-be'
        }

        try:
            with open(file_path, 'rb') as f:
                header = f.read(4)
                for bom, encoding in bom_encodings.items():
                    if header.startswith(bom):
                        logger.info(f"通过BOM头检测到文件编码: {encoding}")
                        return encoding
        except Exception as e:
            logger.error(f"BOM头检测失败: {str(e)}")

        # 2. 使用chardet检测
        try:
            with open(file_path, 'rb') as f:
                # 读取更多数据以提高准确性
                raw_data = f.read(50000)  # 50KB
                result = chardet.detect(raw_data)
                encoding = result['encoding'] or 'utf-8'
                confidence = result['confidence']

                # 如果检测到ascii，则使用utf-8（因为ascii是utf-8的子集）
                if encoding.lower() == 'ascii':
                    encoding = 'utf-8'

                logger.info(f"检测到文件编码: {encoding} (置信度: {confidence:.2f})")

                # 如果置信度低于80%，尝试智能选择
                if confidence < 0.8:
                    return self.select_best_encoding(file_path, raw_data)

                return encoding
        except Exception as e:
            logger.error(f"编码检测失败: {str(e)}，默认使用 utf-8")
            return 'utf-8'

    def select_best_encoding(self, file_path, sample_data):
        """智能选择最佳编码"""
        best_encoding = 'utf-8'
        best_score = 0

        for encoding in self.backup_encodings:
            if self.stop_requested:
                return best_encoding

            try:
                # 尝试解码样本数据
                decoded = sample_data.decode(encoding, errors='ignore')

                # 计算可打印字符比例
                printable_count = sum(1 for c in decoded if c.isprintable() or c in '\r\n\t')
                score = printable_count / len(decoded) if decoded else 0

                # 检查是否有常见日志特征
                has_log_features = any(
                    prefix in decoded for prefix in self.prefixes
                )

                # 如果有日志特征，提高分数
                if has_log_features:
                    score += 0.3

                logger.info(f"编码 {encoding} 得分: {score:.2f}")

                if score > best_score:
                    best_score = score
                    best_encoding = encoding
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.warning(f"测试编码 {encoding} 失败: {str(e)}")

        logger.info(f"选择最佳编码: {best_encoding} (得分: {best_score:.2f})")
        return best_encoding

    def process_log(self, file_path, progress_callback=None, source_archive=None):
        """处理单个日志文件，提取聊天记录"""
        if self.stop_requested:
            return file_path, [], "处理已停止"

        chat_records = []
        encoding = self.detect_encoding(file_path)
        file_name = os.path.basename(file_path)
        large_file = False
        file_size = os.path.getsize(file_path)
        line_count = self.count_lines(file_path)

        # 检查文件大小是否超过10MB 或 行数超过10万
        if file_size > 10 * 1024 * 1024 or line_count > 100000:
            large_file = True
            logger.warning(f"大文件警告: {file_name} ({file_size / 1024 / 1024:.2f} MB, {line_count} 行)")
            if progress_callback:
                progress_callback(file_name, 0, source_archive, "大文件处理中...")

        try:
            # 检查磁盘空间
            disk_usage = psutil.disk_usage(os.path.dirname(file_path))
            if disk_usage.free < 100 * 1024 * 1024:  # 少于100MB
                error_msg = f"磁盘空间不足: 仅剩 {disk_usage.free / 1024 / 1024:.2f} MB 可用空间"
                logger.error(error_msg)
                return file_path, [], error_msg

            processed_size = 0
            start_time = time.time()
            line_errors = 0
            max_line_errors = 100  # 最大允许的行解码错误数
            line_num = 0

            with open(file_path, 'r', encoding=encoding, errors='replace') as file:
                for line in file:
                    if self.stop_requested:
                        return file_path, [], "处理已停止"

                    line_num += 1

                    # 更新进度 - 更频繁的更新
                    if large_file and line_num % 1000 == 0:
                        progress = min(99, int((processed_size / file_size) * 100))
                        if progress_callback:
                            status = "大文件处理中..." if large_file else "处理中..."
                            progress_callback(file_name, progress, source_archive, status)

                    try:
                        line_size = len(line.encode(encoding))
                    except:
                        line_size = 100  # 默认值
                    processed_size += line_size

                    # 检查行是否包含任何前缀
                    for prefix in self.prefixes:
                        if prefix in line:
                            # 提取前缀之后的内容
                            content = line.split(prefix, 1)[1].strip()

                            # 检查内容格式是否符合聊天记录特征
                            if content.startswith("<") or content.startswith("["):
                                chat_records.append(line.strip())
                            break  # 匹配到一个前缀就停止检查

                    # 如果行解码错误太多，尝试备选编码
                    if '\ufffd' in line:  # Unicode替换字符
                        line_errors += 1
                        if line_errors > max_line_errors:
                            logger.warning(f"文件 '{file_name}' 检测到过多解码错误，尝试备选编码")
                            return self.try_backup_encodings(file_path, progress_callback, source_archive, large_file)

            # 处理完成，更新进度为100%
            if progress_callback:
                status = "大文件处理完成" if large_file else "处理完成"
                progress_callback(file_name, 100, source_archive, status)

            # 记录处理时间
            process_time = time.time() - start_time
            logger.info(f"文件 '{file_name}' 处理完成 - 记录数: {len(chat_records)} - 耗时: {process_time:.2f}秒")

        except UnicodeDecodeError as ude:
            error_msg = f"解码错误: {str(ude)}"
            logger.error(error_msg)
            # 尝试使用备份编码
            return self.try_backup_encodings(file_path, progress_callback, source_archive, large_file)
        except Exception as e:
            error_msg = f"处理文件 {file_name} 时出错: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return file_path, [], error_msg

        return file_path, chat_records, None

    def try_backup_encodings(self, file_path, progress_callback=None, source_archive=None, large_file=False):
        """尝试使用备选编码处理文件"""
        if self.stop_requested:
            return file_path, [], "处理已停止"

        file_name = os.path.basename(file_path)
        logger.info(f"为文件 '{file_name}' 尝试备选编码...")

        file_size = os.path.getsize(file_path)
        best_records = []
        best_encoding = None

        for encoding in self.backup_encodings:
            if self.stop_requested:
                break

            try:
                chat_records = []
                logger.info(f"尝试编码: {encoding}")
                processed_size = 0
                line_errors = 0
                max_line_errors = 50  # 最大允许的行解码错误数
                line_num = 0

                with open(file_path, 'r', encoding=encoding, errors='replace') as file:
                    for line in file:
                        if self.stop_requested:
                            break

                        line_num += 1

                        # 更新进度
                        if large_file and line_num % 1000 == 0:
                            progress = min(99, int((processed_size / file_size) * 100))
                            if progress_callback:
                                status = "大文件处理中..." if large_file else "处理中..."
                                progress_callback(file_name, progress, source_archive, status)

                        try:
                            line_size = len(line.encode(encoding))
                        except:
                            line_size = 100  # 默认值
                        processed_size += line_size

                        # 检查是否有太多解码错误
                        if '\ufffd' in line:  # Unicode替换字符
                            line_errors += 1
                            if line_errors > max_line_errors:
                                raise UnicodeDecodeError(f"过多解码错误 ({line_errors})", b'', 0, 0,
                                                         "行包含过多无效字符")

                        # 检查行是否包含任何前缀
                        for prefix in self.prefixes:
                            if prefix in line:
                                content = line.split(prefix, 1)[1].strip()
                                if content.startswith("<") or content.startswith("["):
                                    chat_records.append(line.strip())
                                break

                # 记录最佳结果
                if len(chat_records) > len(best_records):
                    best_records = chat_records
                    best_encoding = encoding
                    logger.info(f"使用 {encoding} 编码找到 {len(chat_records)} 条记录 (当前最佳)")

            except UnicodeDecodeError as ude:
                logger.warning(f"编码 {encoding} 解码失败: {str(ude)}")
            except Exception as e:
                logger.error(f"使用 {encoding} 编码失败: {str(e)}", exc_info=True)

        if best_records:
            logger.info(f"最终选择编码: {best_encoding} 找到 {len(best_records)} 条记录")
            if progress_callback:
                status = "大文件处理完成" if large_file else "处理完成"
                progress_callback(file_name, 100, source_archive, status)
            return file_path, best_records, None

        error_msg = f"所有备选编码均失败: {', '.join(self.backup_encodings)}"
        logger.error(error_msg)
        return file_path, [], error_msg


class ChatExtractorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Minecraft聊天记录提取器")
        self.root.geometry("950x800")
        self.root.configure(bg='#f0f0f0')

        # 设置应用图标（如果有）
        try:
            self.root.iconbitmap('app_icon.ico')
        except:
            pass

        # 初始化处理器和任务队列
        self.processor = LogProcessor()
        self.task_queue = queue.Queue()
        self.results = {}
        self.all_results = {}  # 存储所有处理结果
        self.results_by_id = {}  # 存储每个树ID对应的记录
        self.progress_data = {}  # 存储每个文件的处理进度
        self.currently_processing = 0
        self.total_files = 0
        self.processed_files = 0
        self.temp_dir = None  # 临时目录用于存储解压文件
        self.start_time = None  # 处理开始时间
        self.stop_requested = False  # 停止处理标志
        self.estimated_times = {}  # 存储每个文件的预估时间
        self.file_sizes = {}  # 存储文件大小
        self.completion_shown = False  # 标记完成信息是否已显示
        self.filepath_to_treeid = {}  # 文件路径到树ID的映射
        self.file_conflict_policy = {}  # 文件冲突处理策略
        self.always_apply = None  # 总是应用的选择
        self.worker_thread = None  # 工作线程
        self.copy_dir = None  # 文件复制目录

        # 创建UI
        self.create_widgets()

        # 启动结果处理线程
        self.process_results()

        # 记录启动信息
        logger.info("=" * 80)
        logger.info("Minecraft聊天记录提取器已启动")
        logger.info(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"前缀列表: {self.processor.prefixes}")
        logger.info("=" * 80)

    def create_widgets(self):
        # 创建主框架
        main_frame = tk.Frame(self.root, bg='#f0f0f0', padx=15, pady=15)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 标题
        title_label = tk.Label(
            main_frame,
            text="Minecraft聊天记录提取器",
            font=("Microsoft YaHei", 16, "bold"),
            bg='#f0f0f0',
            fg='#2c3e50'
        )
        title_label.pack(pady=(0, 15))

        # 文件类型选择框架
        type_frame = tk.Frame(main_frame, bg='#f0f0f0')
        type_frame.pack(fill=tk.X, pady=(0, 10))

        # 文件类型标签
        type_label = tk.Label(
            type_frame,
            text="处理优先级:",
            font=("Microsoft YaHei", 10),
            bg='#f0f0f0'
        )
        type_label.pack(side=tk.LEFT, padx=(0, 5))

        # 文件类型选择下拉菜单
        self.file_priority = tk.StringVar(value="先处理小文件")
        priority_combo = ttk.Combobox(
            type_frame,
            textvariable=self.file_priority,
            values=["先处理小文件", "先处理大文件", "按文件名顺序"],
            state="readonly",
            width=20,
            font=("Microsoft YaHei", 10)
        )
        priority_combo.pack(side=tk.LEFT)

        # 操作按钮框架
        button_frame = tk.Frame(main_frame, bg='#f0f0f0')
        button_frame.pack(fill=tk.X, pady=(0, 15))

        # 文件选择按钮
        file_btn = tk.Button(
            button_frame,
            text="选择文件",
            command=self.select_file,
            font=("Microsoft YaHei", 10),
            bg='#3498db',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        file_btn.pack(side=tk.LEFT, padx=5)

        # 文件夹选择按钮
        folder_btn = tk.Button(
            button_frame,
            text="选择文件夹",
            command=self.select_folder,
            font=("Microsoft YaHei", 10),
            bg='#2ecc71',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        folder_btn.pack(side=tk.LEFT, padx=5)

        # 清除按钮
        clear_btn = tk.Button(
            button_frame,
            text="清除所有结果",
            command=self.clear_results,
            font=("Microsoft YaHei", 10),
            bg='#e74c3c',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        clear_btn.pack(side=tk.RIGHT, padx=5)

        # 停止处理按钮 (初始禁用)
        self.stop_btn = tk.Button(
            button_frame,
            text="停止处理",
            command=self.stop_processing,
            state=tk.DISABLED,
            font=("Microsoft YaHei", 10),
            bg='#f39c12',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        self.stop_btn.pack(side=tk.RIGHT, padx=5)

        # 清除空记录按钮
        clear_empty_btn = tk.Button(
            button_frame,
            text="清除空记录",
            command=self.clear_empty_records,
            font=("Microsoft YaHei", 10),
            bg='#9b59b6',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        clear_empty_btn.pack(side=tk.RIGHT, padx=5)

        # 进度条框架
        progress_frame = tk.LabelFrame(
            main_frame,
            text="处理进度",
            font=("Microsoft YaHei", 10),
            bg='#f0f0f0',
            padx=10,
            pady=10
        )
        progress_frame.pack(fill=tk.X, pady=(0, 15))

        # 总进度标签
        self.total_progress_label = tk.Label(
            progress_frame,
            text="等待处理文件...",
            font=("Microsoft YaHei", 9),
            bg='#f0f0f0',
            anchor=tk.W
        )
        self.total_progress_label.pack(fill=tk.X, pady=(0, 5))

        # 总进度条
        self.total_progress = ttk.Progressbar(
            progress_frame,
            orient=tk.HORIZONTAL,
            length=100,
            mode='determinate'
        )
        self.total_progress.pack(fill=tk.X, pady=(0, 10))

        # 当前文件进度标签
        self.current_file_label = tk.Label(
            progress_frame,
            text="当前文件: -",
            font=("Microsoft YaHei", 9),
            bg='#f0f0f0',
            anchor=tk.W
        )
        self.current_file_label.pack(fill=tk.X, pady=(0, 5))

        # 时间预估标签
        self.time_estimate_label = tk.Label(
            progress_frame,
            text="预计时间: 计算中...",
            font=("Microsoft YaHei", 9),
            bg='#f0f0f0',
            anchor=tk.W
        )
        self.time_estimate_label.pack(fill=tk.X, pady=(0, 5))

        # 当前文件进度条
        self.file_progress = ttk.Progressbar(
            progress_frame,
            orient=tk.HORIZONTAL,
            length=100,
            mode='determinate'
        )
        self.file_progress.pack(fill=tk.X)

        # 结果显示区域框架
        result_frame = tk.LabelFrame(
            main_frame,
            text="提取结果 (按住Ctrl键多选)",
            font=("Microsoft YaHei", 10),
            bg='#f0f0f0',
            padx=10,
            pady=10
        )
        result_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # 创建显示选项框架
        display_frame = tk.Frame(result_frame, bg='#f0f0f0')
        display_frame.pack(fill=tk.X, pady=(0, 5))

        # 显示空记录复选框
        self.show_empty_var = tk.BooleanVar(value=False)
        show_empty_cb = tk.Checkbutton(
            display_frame,
            text="显示空记录",
            variable=self.show_empty_var,
            command=self.update_display,
            bg='#f0f0f0',
            font=("Microsoft YaHei", 9)
        )
        show_empty_cb.pack(side=tk.LEFT, padx=5)

        # 排序方式标签
        sort_label = tk.Label(
            display_frame,
            text="排序方式:",
            bg='#f0f0f0',
            font=("Microsoft YaHei", 9)
        )
        sort_label.pack(side=tk.LEFT, padx=(10, 5))

        # 排序方式下拉框
        self.sort_method = tk.StringVar(value="记录数")
        sort_combo = ttk.Combobox(
            display_frame,
            textvariable=self.sort_method,
            values=["记录数", "文件名", "状态"],
            state="readonly",
            width=10,
            font=("Microsoft YaHei", 9)
        )
        sort_combo.pack(side=tk.LEFT)
        sort_combo.bind("<<ComboboxSelected>>", self.update_display)

        # 创建Treeview用于显示结果
        self.tree = ttk.Treeview(
            result_frame,
            columns=("file", "status", "records"),
            show="headings",
            selectmode="extended"
        )

        # 设置列
        self.tree.heading("file", text="文件名")
        self.tree.column("file", width=350, anchor=tk.W)
        self.tree.heading("status", text="状态")
        self.tree.column("status", width=120, anchor=tk.CENTER)
        self.tree.heading("records", text="记录数")
        self.tree.column("records", width=80, anchor=tk.CENTER)

        # 添加滚动条
        scrollbar_y = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.tree.yview)
        scrollbar_x = ttk.Scrollbar(result_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

        # 布局
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)

        # 导出按钮框架
        export_frame = tk.Frame(main_frame, bg='#f0f0f0')
        export_frame.pack(fill=tk.X, pady=(5, 10))

        # 导出按钮 (初始禁用)
        self.export_all_btn = tk.Button(
            export_frame,
            text="导出所有聊天记录",
            command=self.export_all,
            state=tk.DISABLED,
            font=("Microsoft YaHei", 10),
            bg='#9b59b6',
            fg='white',
            relief=tk.FLAT,
            padx=15
        )
        self.export_all_btn.pack(side=tk.LEFT, padx=5)

        self.export_selected_btn = tk.Button(
            export_frame,
            text="导出选定聊天记录",
            command=self.export_selected,
            state=tk.DISABLED,
            font=("Microsoft YaHei", 10),
            bg='#f39c12',
            fg='white',
            relief=tk.FLAT,
            padx=15
        )
        self.export_selected_btn.pack(side=tk.LEFT, padx=5)

        # 合并按钮
        self.merge_all_btn = tk.Button(
            export_frame,
            text="合并全部聊天记录",
            command=lambda: self.merge_records(selected=False),
            state=tk.DISABLED,
            font=("Microsoft YaHei", 10),
            bg='#1abc9c',
            fg='white',
            relief=tk.FLAT,
            padx=15
        )
        self.merge_all_btn.pack(side=tk.LEFT, padx=5)

        # 合并选定按钮
        self.merge_selected_btn = tk.Button(
            export_frame,
            text="合并选定聊天记录",
            command=lambda: self.merge_records(selected=True),
            state=tk.DISABLED,
            font=("Microsoft YaHei", 10),
            bg='#16a085',
            fg='white',
            relief=tk.FLAT,
            padx=15
        )
        self.merge_selected_btn.pack(side=tk.LEFT, padx=5)

        # 状态栏
        status_bar = tk.Frame(self.root, bd=1, relief=tk.SUNKEN)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        self.status_var = tk.StringVar()
        self.status_var.set("就绪 - 等待选择文件")
        status_label = tk.Label(
            status_bar,
            textvariable=self.status_var,
            anchor=tk.W,
            font=("Microsoft YaHei", 9),
            bg='#ecf0f1',
            fg='#2c3e50'
        )
        status_label.pack(fill=tk.X, padx=5)

        # 绑定双击事件查看详情
        self.tree.bind("<Double-1>", self.show_chat_details)

    def update_progress(self, file_name, progress, source_archive=None, status="处理中..."):
        """更新文件处理进度"""
        # 更新UI
        if source_archive:
            display_name = f"{os.path.basename(source_archive)}/{file_name}"
        else:
            display_name = file_name

        # 对于大文件，使用黄色文本
        if "大文件" in status:
            self.current_file_label.config(text=f"当前文件: {display_name} ({progress}%)", fg="orange")
        else:
            self.current_file_label.config(text=f"当前文件: {display_name} ({progress}%)", fg="black")

        self.file_progress["value"] = progress

        # 记录进度
        self.progress_data[display_name] = progress

        # 更新总进度
        if self.total_files > 0:
            # 计算平均进度
            total_progress = sum(self.progress_data.values()) / self.total_files
            completed = sum(1 for p in self.progress_data.values() if p == 100)
            self.total_progress["value"] = total_progress
            self.total_progress_label.config(
                text=f"总进度: {completed}/{self.total_files} 个文件 ({total_progress:.1f}%)")

            # 更新时间预估
            if self.start_time:
                elapsed = time.time() - self.start_time
                if completed > 0:
                    time_per_file = elapsed / completed
                    remaining_files = self.total_files - completed
                    remaining_time = time_per_file * remaining_files

                    # 格式化剩余时间
                    if remaining_time > 60:
                        mins = int(remaining_time // 60)
                        secs = int(remaining_time % 60)
                        time_str = f"{mins}分{secs}秒"
                    else:
                        time_str = f"{int(remaining_time)}秒"

                    self.time_estimate_label.config(text=f"预计剩余时间: {time_str}")

    def update_status(self, message):
        """更新状态栏信息"""
        self.status_var.set(message)
        logger.info(f"[状态] {message}")

    def select_file(self):
        """选择单个日志文件或压缩文件"""
        self.update_status("正在选择文件...")

        filetypes = [("日志文件", "*.log *.gz"), ("所有文件", "*.*")]

        file_path = filedialog.askopenfilename(filetypes=filetypes)

        if not file_path:
            self.update_status("已取消文件选择")
            return

        self.process_files([file_path])

    def select_folder(self):
        """选择包含日志文件或压缩文件的文件夹"""
        self.update_status("正在选择文件夹...")
        folder_path = filedialog.askdirectory()
        if not folder_path:
            self.update_status("已取消文件夹选择")
            return

        # 收集所有日志和压缩文件
        files = []
        for root, _, filenames in os.walk(folder_path):
            for filename in filenames:
                if filename.lower().endswith(('.log', '.log.gz', '.gz')):
                    files.append(os.path.join(root, filename))

        if not files:
            self.update_status("警告: 未找到文件")
            messagebox.showinfo("提示", "未找到任何文件")
            return

        self.update_status(f"找到 {len(files)} 个文件")
        self.process_files(files)

    def copy_to_temp_dir(self, file_paths):
        """复制文件到临时目录"""
        # 创建临时目录
        if not self.copy_dir:
            self.copy_dir = os.path.join(os.getcwd(), "temp")
            os.makedirs(self.copy_dir, exist_ok=True)
            set_hidden(self.copy_dir)  # 设置为隐藏
            logger.info(f"创建临时目录: {self.copy_dir}")

        copied_files = []

        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            dest_path = os.path.join(self.copy_dir, file_name)

            try:
                # 检查是否已存在
                if os.path.exists(dest_path):
                    # 检查内容是否相同
                    if self.file_content_identical(file_path, dest_path):
                        copied_files.append(dest_path)
                        continue

                    # 生成唯一文件名
                    base, ext = os.path.splitext(file_name)
                    counter = 1
                    while True:
                        new_name = f"{base}_{counter}{ext}"
                        new_dest = os.path.join(self.copy_dir, new_name)
                        if not os.path.exists(new_dest):
                            dest_path = new_dest
                            break
                        counter += 1

                # 复制文件
                shutil.copy2(file_path, dest_path)
                copied_files.append(dest_path)
                logger.info(f"复制文件: {file_path} -> {dest_path}")
            except Exception as e:
                logger.error(f"复制文件失败: {file_path} -> {dest_path}: {str(e)}")
                copied_files.append(file_path)  # 使用原始路径

        return copied_files

    def file_content_identical(self, file1, file2):
        """检查两个文件内容是否相同"""
        try:
            # 比较文件大小
            if os.path.getsize(file1) != os.path.getsize(file2):
                return False

            # 比较文件哈希
            hash1 = self.calculate_file_hash(file1)
            hash2 = self.calculate_file_hash(file2)
            return hash1 == hash2
        except:
            return False

    def calculate_file_hash(self, file_path):
        """计算文件哈希值"""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                hasher.update(chunk)
        return hasher.hexdigest()

    def process_files(self, file_paths):
        """启动线程处理多个文件"""
        # 重置状态
        self.stop_requested = False
        self.processor.set_stop_requested(False)
        self.start_time = time.time()
        self.total_files = len(file_paths)
        self.processed_files = 0
        self.currently_processing = 0
        self.progress_data = {}
        self.file_sizes = {}
        self.completion_shown = False

        # 复制文件到临时目录
        copied_files = self.copy_to_temp_dir(file_paths)
        logger.info(f"已将 {len(copied_files)} 个文件复制到临时目录")

        # 根据优先级排序文件
        if self.file_priority.get() == "先处理小文件":
            # 按文件大小升序排序
            copied_files.sort(key=lambda p: os.path.getsize(p))
            logger.info("按文件大小升序排序 (先处理小文件)")
        elif self.file_priority.get() == "先处理大文件":
            # 按文件大小降序排序
            copied_files.sort(key=lambda p: os.path.getsize(p), reverse=True)
            logger.info("按文件大小降序排序 (先处理大文件)")
        else:
            # 按文件名排序
            copied_files.sort(key=lambda p: os.path.basename(p))
            logger.info("按文件名排序")

        # 记录文件大小
        for path in copied_files:
            self.file_sizes[path] = os.path.getsize(path)

        # 重置进度条
        self.total_progress["value"] = 0
        self.file_progress["value"] = 0
        self.total_progress_label.config(text=f"总进度: 0/{self.total_files} 个文件 (0%)")
        self.current_file_label.config(text="当前文件: -")
        self.time_estimate_label.config(text="预计剩余时间: 计算中...")

        self.update_status(f"正在处理 {self.total_files} 个文件...")

        # 清除之前的结果
        for item in self.tree.get_children():
            self.tree.delete(item)

        # 添加文件到处理队列
        for file_path in copied_files:
            file_name = os.path.basename(file_path)

            # 压缩文件
            if file_path.lower().endswith(('.gz', '.log.gz')):
                # 在Treeview中添加压缩文件项
                self.tree.insert("", tk.END, iid=file_path, values=(f"[压缩] {file_name}", "等待解压...", "0"))
                # 添加到任务队列
                self.task_queue.put(("extract", file_path))
            else:
                # 添加普通日志文件
                self.tree.insert("", tk.END, iid=file_path, values=(file_name, "等待中...", "0"))
                # 添加到任务队列
                self.task_queue.put(("process", file_path, None))

        # 启用停止按钮
        self.stop_btn.config(state=tk.NORMAL)

        # 如果工作线程未启动，则启动它
        if not hasattr(self, 'worker_thread') or not self.worker_thread.is_alive():
            self.worker_thread = threading.Thread(target=self.worker, daemon=True)
            self.worker_thread.start()

    def stop_processing(self):
        """停止当前处理"""
        self.stop_requested = True
        self.processor.set_stop_requested(True)
        self.update_status("正在停止处理...")

        # 清空任务队列
        while not self.task_queue.empty():
            try:
                self.task_queue.get_nowait()
            except queue.Empty:
                break

        # 禁用停止按钮
        self.stop_btn.config(state=tk.DISABLED)

        # 更新状态
        if self.processed_files > 0:
            self.update_status(f"处理已停止 - 已完成 {self.processed_files}/{self.total_files} 个文件")
        else:
            self.update_status("处理已停止 - 未完成任何文件")

        # 重置进度条
        self.total_progress["value"] = 0
        self.file_progress["value"] = 0
        self.total_progress_label.config(text=f"已停止 - 完成 {self.processed_files}/{self.total_files} 个文件")
        self.current_file_label.config(text="已停止处理")
        self.time_estimate_label.config(text="预计剩余时间: -")

    def clear_results(self):
        """清除所有结果"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.all_results = {}
        self.results_by_id = {}
        self.progress_data = {}
        self.filepath_to_treeid = {}
        self.total_files = 0
        self.processed_files = 0
        self.currently_processing = 0
        self.completion_shown = False

        # 重置进度条
        self.total_progress["value"] = 0
        self.file_progress["value"] = 0
        self.total_progress_label.config(text="等待处理文件...")
        self.current_file_label.config(text="当前文件: -")
        self.time_estimate_label.config(text="预计剩余时间: -")

        self.export_all_btn.config(state=tk.DISABLED)
        self.export_selected_btn.config(state=tk.DISABLED)
        self.merge_all_btn.config(state=tk.DISABLED)
        self.merge_selected_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.DISABLED)
        self.update_status("已清除所有结果")

        # 清理临时目录
        self.cleanup_temp_dirs()

    def cleanup_temp_dirs(self):
        """清理所有临时目录"""
        # 清理复制目录
        if self.copy_dir and os.path.exists(self.copy_dir):
            try:
                shutil.rmtree(self.copy_dir)
                logger.info(f"已清理复制目录: {self.copy_dir}")
                self.copy_dir = None
            except Exception as e:
                logger.error(f"清理复制目录失败: {str(e)}")

        # 清理解压目录
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                logger.info(f"已清理临时目录: {self.temp_dir}")
                self.temp_dir = None
            except Exception as e:
                logger.error(f"清理临时目录失败: {str(e)}")

    def clear_empty_records(self):
        """清除没有聊天记录的文件"""
        empty_items = []
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            # 检查记录数是否为0
            if values[2] == "0" or values[2] == "":
                empty_items.append(item)

        for item in empty_items:
            file_path = item
            # 从树中删除
            self.tree.delete(item)
            # 从结果中删除
            if file_path in self.all_results:
                del self.all_results[file_path]
            if file_path in self.results_by_id:
                del self.results_by_id[file_path]
            if file_path in self.filepath_to_treeid:
                del self.filepath_to_treeid[file_path]

        count = len(empty_items)
        self.update_status(f"已清除 {count} 个没有聊天记录的文件")
        messagebox.showinfo("清除完成", f"已清除 {count} 个没有聊天记录的文件")

    def extract_gz_file(self, gz_path):
        """解压.gz文件到临时目录"""
        try:
            # 检查磁盘空间
            disk_usage = psutil.disk_usage(os.path.dirname(gz_path))
            if disk_usage.free < 100 * 1024 * 1024:  # 少于100MB
                error_msg = f"磁盘空间不足: 仅剩 {disk_usage.free / 1024 / 1024:.2f} MB 可用空间"
                logger.error(error_msg)
                return None, error_msg

            # 创建临时目录
            if self.temp_dir is None:
                self.temp_dir = tempfile.mkdtemp(prefix="minecraft_logs_")
                logger.info(f"创建临时目录: {self.temp_dir}")

            # 创建解压目标目录
            extract_dir = os.path.join(self.temp_dir, os.path.basename(gz_path) + "_extracted")
            os.makedirs(extract_dir, exist_ok=True)

            # 解压后的文件路径
            file_name = os.path.basename(gz_path)
            if file_name.endswith('.log.gz'):
                file_name = file_name[:-7] + '.log'
            elif file_name.endswith('.gz'):
                file_name = file_name[:-3]
            extract_path = os.path.join(extract_dir, file_name)

            # 解压文件
            with gzip.open(gz_path, 'rb') as f_in:
                with open(extract_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            logger.info(f"已解压: {gz_path} -> {extract_path}")
            return extract_path, None
        except Exception as e:
            error_msg = f"解压文件 {gz_path} 失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return None, error_msg

    def worker(self):
        """工作线程处理日志文件"""
        logger.info("工作线程已启动")
        while not self.stop_requested:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # 终止信号
                    break

                task_type = task[0]

                if task_type == "extract":
                    # 解压任务
                    gz_path = task[1]
                    file_name = os.path.basename(gz_path)
                    self.update_status(f"正在解压文件: {file_name}")

                    # 更新Treeview状态
                    for item in self.tree.get_children():
                        if item == gz_path:
                            self.tree.item(item, values=(f"[压缩] {file_name}", "解压中...", "0"))
                            break

                    # 解压文件
                    extracted_path, error = self.extract_gz_file(gz_path)

                    if extracted_path:
                        # 添加处理任务
                        self.task_queue.put(("process", extracted_path, gz_path))
                        # 更新Treeview状态
                        for item in self.tree.get_children():
                            if item == gz_path:
                                self.tree.item(item, values=(f"[压缩] {file_name}", "解压完成", "0"))
                                break
                    else:
                        # 解压失败
                        for item in self.tree.get_children():
                            if item == gz_path:
                                self.tree.item(item, values=(f"[压缩] {file_name}", f"解压失败: {error}", "0"))
                                break

                elif task_type == "process":
                    # 处理任务
                    file_path = task[1]
                    source_archive = task[2]  # 对于解压文件，记录源压缩文件
                    file_name = os.path.basename(file_path)
                    self.currently_processing += 1

                    if source_archive:
                        display_name = f"{os.path.basename(source_archive)}/{file_name}"
                    else:
                        display_name = file_name

                    self.update_status(f"正在处理文件: {display_name} ({self.currently_processing}/{self.total_files})")

                    # 更新Treeview状态
                    for item in self.tree.get_children():
                        if item == file_path or (source_archive and item == source_archive):
                            if source_archive:
                                # 对于解压文件，更新压缩文件项
                                self.tree.item(item,
                                               values=(f"[压缩] {os.path.basename(source_archive)}", "处理中...", "0"))
                            else:
                                # 对于普通文件
                                self.tree.item(item, values=(file_name, "处理中...", "0"))
                            break

                    # 处理日志文件
                    _, records, error = self.processor.process_log(
                        file_path,
                        progress_callback=lambda fn, p, sa, s: self.update_progress(fn, p, sa, s),
                        source_archive=source_archive
                    )

                    # 将结果放入队列供主线程处理
                    self.results[file_path] = {
                        "records": records,
                        "error": error,
                        "status": "成功" if not error else "失败",
                        "source_archive": source_archive
                    }

                    # 存储到所有结果中
                    self.all_results[file_path] = records

                    # 存储到结果映射
                    tree_id = source_archive if source_archive else file_path
                    self.results_by_id[tree_id] = records
                    self.filepath_to_treeid[file_path] = tree_id

                    self.processed_files += 1

                self.task_queue.task_done()
            except queue.Empty:
                # 队列为空，退出线程
                break
            except Exception as e:
                logger.error(f"工作线程异常: {str(e)}", exc_info=True)

    def validate_content(self):
        """内容校验阶段 - 检查乱码并尝试重新处理"""
        logger.info("开始内容校验阶段...")
        revalidated = 0

        for tree_id, records in list(self.results_by_id.items()):
            if self.stop_requested:
                break

            # 检查是否有乱码
            has_garbled = any('\ufffd' in record for record in records)
            if not has_garbled:
                continue

            logger.warning(f"检测到乱码: {tree_id}")

            # 获取原始文件路径
            file_path = None
            for path, tid in self.filepath_to_treeid.items():
                if tid == tree_id:
                    file_path = path
                    break

            if not file_path:
                logger.error(f"无法找到原始文件路径: {tree_id}")
                continue

            # 尝试重新处理
            logger.info(f"尝试重新处理文件: {file_path}")
            _, new_records, error = self.processor.process_log(
                file_path,
                progress_callback=None,
                source_archive=None
            )

            if error:
                logger.error(f"重新处理失败: {error}")
                # 更新状态为失败
                for item in self.tree.get_children():
                    if item == tree_id:
                        self.tree.item(item, values=(self.tree.item(item, "values")[0], "失败: 乱码无法修复", "0"))
                        break
            else:
                # 检查是否还有乱码
                still_has_garbled = any('\ufffd' in record for record in new_records)
                if still_has_garbled:
                    logger.warning(f"重新处理后仍存在乱码: {tree_id}")
                    # 更新状态为失败
                    for item in self.tree.get_children():
                        if item == tree_id:
                            self.tree.item(item, values=(self.tree.item(item, "values")[0], "失败: 乱码无法修复", "0"))
                            break
                else:
                    # 更新记录
                    self.results_by_id[tree_id] = new_records
                    self.all_results[file_path] = new_records

                    # 更新树视图
                    for item in self.tree.get_children():
                        if item == tree_id:
                            values = self.tree.item(item, "values")
                            self.tree.item(item, values=(values[0], "成功(重新处理)", str(len(new_records))))
                            break

                    revalidated += 1
                    logger.info(f"重新处理成功: {tree_id} - 新记录数: {len(new_records)}")

        logger.info(f"内容校验完成: 重新处理 {revalidated} 个文件")
        return revalidated

    def deduplicate_records(self):
        """去重聊天记录"""
        if self.stop_requested:
            return 0

        logger.info("开始去重聊天记录...")
        deduplicated_count = 0

        for tree_id, records in list(self.results_by_id.items()):
            if self.stop_requested:
                break

            if not records:
                continue

            # 使用集合去重但保持顺序
            seen = set()
            deduplicated_records = []
            for record in records:
                if record not in seen:
                    seen.add(record)
                    deduplicated_records.append(record)

            # 更新记录
            deduplicated = len(records) - len(deduplicated_records)
            self.results_by_id[tree_id] = deduplicated_records
            deduplicated_count += deduplicated

            # 更新all_results
            if tree_id in self.filepath_to_treeid.values():
                for file_path, tid in self.filepath_to_treeid.items():
                    if tid == tree_id:
                        self.all_results[file_path] = deduplicated_records
                        break

        logger.info(f"去重完成，共移除 {deduplicated_count} 条重复记录")
        return deduplicated_count

    def update_tree_after_deduplicate(self, deduplicated_count):
        """去重后更新树显示"""
        for tree_id in self.tree.get_children():
            records = self.results_by_id.get(tree_id, [])
            current_values = self.tree.item(tree_id, "values")
            new_values = (current_values[0], current_values[1], str(len(records)))
            self.tree.item(tree_id, values=new_values)

        # 更新显示
        self.update_display()
        self.update_status(f"去重完成，移除 {deduplicated_count} 条重复记录")

    def process_results(self):
        """主线程定期检查并更新结果"""
        for file_path, result in list(self.results.items()):
            # 更新Treeview
            record_count = len(result["records"])
            status = result["status"]
            source_archive = result.get("source_archive")

            if source_archive:
                # 对于解压文件，显示源压缩文件名
                display_name = f"[压缩] {os.path.basename(source_archive)}"
                # 查找并更新对应的树项
                for item in self.tree.get_children():
                    if item == source_archive:
                        self.tree.item(item, values=(display_name, status, record_count))
                        break
            else:
                # 普通文件
                file_name = os.path.basename(file_path)
                # 查找并更新对应的树项
                for item in self.tree.get_children():
                    if item == file_path:
                        self.tree.item(item, values=(file_name, status, record_count))
                        break

            # 从临时结果中移除
            del self.results[file_path]

        # 如果有任何处理完成的结果，启用导出按钮
        if self.tree.get_children():
            self.export_all_btn.config(state=tk.NORMAL)
            self.export_selected_btn.config(state=tk.NORMAL)
            self.merge_all_btn.config(state=tk.NORMAL)
            self.merge_selected_btn.config(state=tk.NORMAL)

            # 检查是否所有文件都处理完成
            completed = sum(1 for item in self.tree.get_children()
                            if self.tree.item(item, "values")[1] in ["成功", "成功(重新处理)", "失败"])

            # 计算总进度
            total_progress = sum(self.progress_data.values()) / self.total_files if self.total_files > 0 else 0
            self.total_progress["value"] = total_progress

            if completed == self.total_files and self.total_files > 0 and not self.completion_shown:
                success_count = sum(1 for item in self.tree.get_children()
                                    if self.tree.item(item, "values")[1] in ["成功", "成功(重新处理)"])
                failed_count = self.total_files - success_count

                # 计算总处理时间
                process_time = time.time() - self.start_time
                mins = int(process_time // 60)
                secs = int(process_time % 60)
                time_str = f"{mins}分{secs}秒" if mins > 0 else f"{secs}秒"

                # 计算总记录数和总字数
                total_records = 0
                total_chars = 0
                for records in self.all_results.values():
                    total_records += len(records)
                    for record in records:
                        total_chars += len(record)

                # 显示完成信息
                message = (
                    f"处理完成!\n\n"
                    f"文件总数: {self.total_files}\n"
                    f"成功处理: {success_count}\n"
                    f"处理失败: {failed_count}\n"
                    f"总记录数: {total_records}\n"
                    f"总字数: {total_chars}\n"
                    f"处理时间: {time_str}"
                )
                messagebox.showinfo("处理完成", message)
                # 标记完成信息已显示
                self.completion_shown = True

                # 启动内容校验阶段
                if not self.stop_requested:
                    validate_thread = threading.Thread(
                        target=self.validate_and_deduplicate,
                        daemon=True
                    )
                    validate_thread.start()

                # 更新显示
                self.update_display()

                # 重置进度条
                self.total_progress["value"] = 100
                self.file_progress["value"] = 0
                self.current_file_label.config(text="处理完成", fg="black")
                self.time_estimate_label.config(text="处理完成")
                self.stop_btn.config(state=tk.DISABLED)

        # 每500ms检查一次
        self.root.after(500, self.process_results)

    def validate_and_deduplicate(self):
        """在后台线程中执行内容校验和去重"""
        # 内容校验
        revalidated = self.validate_content()

        # 去重
        if not self.stop_requested:
            deduplicated_count = self.deduplicate_records()
            self.root.after(0, lambda: self.update_tree_after_deduplicate(deduplicated_count))

            # 更新状态
            self.root.after(0, lambda: self.update_status(
                f"处理完成: 重新处理 {revalidated} 个文件, 移除 {deduplicated_count} 条重复记录"
            ))

    def update_display(self, event=None):
        """更新结果列表显示"""
        # 获取当前所有项目
        items = []
        for item in self.tree.get_children():
            values = self.tree.item(item, "values")
            file_path = item
            record_count = int(values[2]) if values[2].isdigit() else 0
            items.append({
                "id": item,
                "file": values[0],
                "status": values[1],
                "records": record_count
            })

        # 应用过滤和排序
        if not self.show_empty_var.get():
            items = [item for item in items if item["records"] > 0]

        sort_method = self.sort_method.get()
        if sort_method == "记录数":
            items.sort(key=lambda x: x["records"], reverse=True)
        elif sort_method == "文件名":
            items.sort(key=lambda x: x["file"])
        elif sort_method == "状态":
            items.sort(key=lambda x: x["status"])

        # 清空并重新填充树
        for item in self.tree.get_children():
            self.tree.delete(item)

        for item in items:
            self.tree.insert("", tk.END, iid=item["id"], values=(
                item["file"],
                item["status"],
                item["records"]
            ))

    def show_chat_details(self, event):
        """显示选中文件的聊天详情"""
        selected_items = self.tree.selection()
        if not selected_items:
            return

        # 修复：直接使用选中的项目ID
        tree_id = selected_items[0]

        # 修复：直接从results_by_id获取记录
        records = self.results_by_id.get(tree_id, [])

        if not records:
            messagebox.showinfo("提示", "此文件没有提取到聊天记录")
            return

        # 获取显示名称
        values = self.tree.item(tree_id, "values")
        display_name = values[0]

        # 创建详情窗口
        detail_win = tk.Toplevel(self.root)
        detail_win.title(f"聊天详情 - {display_name}")
        detail_win.geometry("900x600")
        detail_win.configure(bg='#f0f0f0')

        # 主框架
        main_frame = tk.Frame(detail_win, bg='#f0f0f0', padx=15, pady=15)
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 标题
        title_label = tk.Label(
            main_frame,
            text=f"聊天记录: {display_name}",
            font=("Microsoft YaHei", 12, "bold"),
            bg='#f0f0f0',
            fg='#2c3e50'
        )
        title_label.pack(anchor=tk.W, pady=(0, 10))

        # 添加文本框显示聊天记录
        text_frame = tk.LabelFrame(
            main_frame,
            text="聊天内容",
            font=("Microsoft YaHei", 10),
            bg='#f0f0f0'
        )
        text_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        text = tk.Text(
            text_frame,
            wrap=tk.WORD,
            font=("Microsoft YaHei", 10),
            bg='white',
            relief=tk.FLAT,
            padx=10,
            pady=10
        )
        scrollbar = tk.Scrollbar(text_frame, command=text.yview)
        text.config(yscrollcommand=scrollbar.set)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # 插入聊天记录
        for record in records:
            text.insert(tk.END, record + "\n")

        text.config(state=tk.DISABLED)

        # 添加按钮框架
        btn_frame = tk.Frame(main_frame, bg='#f0f0f0')
        btn_frame.pack(fill=tk.X, pady=(5, 0))

        # 导出按钮
        export_btn = tk.Button(
            btn_frame,
            text="导出此文件聊天记录",
            command=lambda: self.export_single_file(tree_id, display_name),
            font=("Microsoft YaHei", 10),
            bg='#9b59b6',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        export_btn.pack(side=tk.LEFT, padx=5)

        # 复制按钮
        copy_btn = tk.Button(
            btn_frame,
            text="复制到剪贴板",
            command=lambda: self.copy_to_clipboard(records),
            font=("Microsoft YaHei", 10),
            bg='#3498db',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        copy_btn.pack(side=tk.LEFT, padx=5)

        # 关闭按钮
        close_btn = tk.Button(
            btn_frame,
            text="关闭",
            command=detail_win.destroy,
            font=("Microsoft YaHei", 10),
            bg='#95a5a6',
            fg='white',
            relief=tk.FLAT,
            padx=10
        )
        close_btn.pack(side=tk.RIGHT, padx=5)

    def copy_to_clipboard(self, records):
        """复制聊天记录到剪贴板"""
        self.root.clipboard_clear()
        self.root.clipboard_append("\n".join(records))
        self.update_status("聊天记录已复制到剪贴板")

    def handle_file_conflict(self, file_path):
        """处理文件冲突"""
        # 检查是否有全局策略
        if self.always_apply:
            return self.always_apply

        # 创建冲突解决对话框
        conflict_win = tk.Toplevel(self.root)
        conflict_win.title("文件冲突")
        conflict_win.geometry("400x200")
        conflict_win.resizable(False, False)
        conflict_win.grab_set()

        # 消息
        msg = f"文件已存在:\n{file_path}\n\n请选择操作:"
        msg_label = tk.Label(conflict_win, text=msg, font=("Microsoft YaHei", 10), padx=10, pady=10)
        msg_label.pack()

        # 选项框架
        btn_frame = tk.Frame(conflict_win)
        btn_frame.pack(pady=10)

        # 覆盖按钮
        overwrite_btn = tk.Button(
            btn_frame,
            text="覆盖",
            command=lambda: self.set_conflict_result(conflict_win, "overwrite"),
            width=10
        )
        overwrite_btn.pack(side=tk.LEFT, padx=5)

        # 跳过按钮
        skip_btn = tk.Button(
            btn_frame,
            text="跳过",
            command=lambda: self.set_conflict_result(conflict_win, "skip"),
            width=10
        )
        skip_btn.pack(side=tk.LEFT, padx=5)

        # 重命名按钮
        rename_btn = tk.Button(
            btn_frame,
            text="重命名",
            command=lambda: self.set_conflict_result(conflict_win, "rename"),
            width=10
        )
        rename_btn.pack(side=tk.LEFT, padx=5)

        # 总是应用复选框
        self.always_var = tk.BooleanVar(value=False)
        always_cb = tk.Checkbutton(
            conflict_win,
            text="总是执行此操作",
            variable=self.always_var,
            font=("Microsoft YaHei", 9)
        )
        always_cb.pack(pady=5)

        # 等待用户选择
        conflict_win.wait_window()

        return self.file_conflict_policy.get(file_path, "skip")

    def set_conflict_result(self, win, action):
        """设置冲突解决结果"""
        if self.always_var.get():
            self.always_apply = action
        self.file_conflict_policy[win] = action
        win.destroy()

    def export_single_file(self, tree_id, display_name=None):
        """导出单个文件的聊天记录"""
        records = self.results_by_id.get(tree_id, [])
        if not records:
            messagebox.showinfo("提示", "此文件没有聊天记录可导出")
            return

        # 创建输出目录
        output_dir = "聊天记录"
        os.makedirs(output_dir, exist_ok=True)

        # 生成输出文件名
        if display_name:
            # 对于解压文件，使用显示名称
            filename = f"[聊天记录]{display_name.replace('/', '_')}.txt"
        else:
            filename = os.path.basename(tree_id)
            filename = f"[聊天记录]{filename}.txt"

        output_path = os.path.join(output_dir, filename)

        # 处理文件冲突
        if os.path.exists(output_path):
            action = self.handle_file_conflict(output_path)
            if action == "skip":
                self.update_status(f"跳过文件: {filename}")
                return
            elif action == "rename":
                base, ext = os.path.splitext(output_path)
                counter = 1
                while True:
                    new_path = f"{base}_{counter}{ext}"
                    if not os.path.exists(new_path):
                        output_path = new_path
                        break
                    counter += 1

        try:
            # 检查磁盘空间
            disk_usage = psutil.disk_usage(os.path.dirname(output_path) or '.')
            if disk_usage.free < 100 * 1024 * 1024:  # 少于100MB
                error_msg = f"磁盘空间不足: 仅剩 {disk_usage.free / 1024 / 1024:.2f} MB 可用空间"
                messagebox.showerror("导出错误", error_msg)
                self.update_status(error_msg)
                return

            # 尝试使用UTF-8编码保存
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(records))
            message = f"成功导出聊天记录到: {output_path}"
            messagebox.showinfo("导出成功", message)
            self.update_status(message)

        except Exception as e:
            error_msg = f"导出失败: {str(e)}"
            messagebox.showerror("导出错误", error_msg)
            self.update_status(error_msg)

    def export_all(self):
        """导出所有聊天记录"""
        self._export_records(self.tree.get_children())

    def export_selected(self):
        """导出选定的聊天记录"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("警告", "你没有选择任何文件")
            return

        self._export_records(selected)

    def _export_records(self, file_ids):
        """实际导出聊天记录的实现"""
        # 创建输出目录
        output_dir = "聊天记录"
        os.makedirs(output_dir, exist_ok=True)

        # 检查磁盘空间
        disk_usage = psutil.disk_usage(os.path.dirname(output_dir) or '.')
        if disk_usage.free < 100 * 1024 * 1024:  # 少于100MB
            error_msg = f"磁盘空间不足: 仅剩 {disk_usage.free / 1024 / 1024:.2f} MB 可用空间"
            messagebox.showerror("导出错误", error_msg)
            self.update_status(error_msg)
            return

        exported_count = 0
        skipped_count = 0
        total_records = 0
        errors = []
        self.stop_export = False  # 导出停止标志

        # 创建进度窗口
        export_win = tk.Toplevel(self.root)
        export_win.title("导出进度")
        export_win.geometry("400x200")
        export_win.resizable(False, False)
        export_win.grab_set()

        # 标题
        title_label = tk.Label(
            export_win,
            text="正在导出聊天记录...",
            font=("Microsoft YaHei", 10, "bold"),
            pady=10
        )
        title_label.pack()

        # 进度标签
        progress_label = tk.Label(
            export_win,
            text="准备中...",
            font=("Microsoft YaHei", 9)
        )
        progress_label.pack()

        # 进度条
        export_progress = ttk.Progressbar(
            export_win,
            orient=tk.HORIZONTAL,
            length=300,
            mode='determinate'
        )
        export_progress.pack(pady=10)
        export_progress["maximum"] = len(file_ids)

        # 停止按钮
        stop_btn = tk.Button(
            export_win,
            text="停止导出",
            command=lambda: setattr(self, 'stop_export', True),
            font=("Microsoft YaHei", 10),
            bg='#e74c3c',
            fg='white'
        )
        stop_btn.pack(pady=5)

        # 更新UI
        export_win.update()

        try:
            for i, file_id in enumerate(file_ids):
                if self.stop_export:
                    break

                records = self.results_by_id.get(file_id, [])

                # 更新进度
                progress = i + 1
                export_progress["value"] = progress

                # 获取显示名称
                values = self.tree.item(file_id, "values")
                display_name = values[0]
                progress_label.config(text=f"正在导出: {display_name} ({progress}/{len(file_ids)})")
                export_win.update()

                if not records:
                    skipped_count += 1
                    continue

                # 生成输出文件名
                output_path = os.path.join(output_dir, f"[聊天记录]{display_name.replace('/', '_')}.txt")

                # 处理文件冲突
                if os.path.exists(output_path):
                    action = self.handle_file_conflict(output_path)
                    if action == "skip":
                        skipped_count += 1
                        continue
                    elif action == "rename":
                        base, ext = os.path.splitext(output_path)
                        counter = 1
                        while True:
                            new_path = f"{base}_{counter}{ext}"
                            if not os.path.exists(new_path):
                                output_path = new_path
                                break
                            counter += 1

                # 写入文件
                try:
                    with open(output_path, 'w', encoding='utf-8') as f:
                        f.write("\n".join(records))
                    exported_count += 1
                    total_records += len(records)
                except Exception as e:
                    error_msg = f"导出失败: {display_name} - {str(e)}"
                    errors.append(error_msg)

                # 短暂暂停，让UI更新
                time.sleep(0.05)

        finally:
            # 关闭进度窗口
            export_win.destroy()

        # 显示结果消息
        if exported_count > 0:
            message = f"成功导出 {exported_count} 个文件，共 {total_records} 条聊天记录"
            if skipped_count > 0:
                message += f"\n跳过 {skipped_count} 个文件"
            if errors:
                message += f"\n\n导出失败 {len(errors)} 个文件:\n" + "\n".join(errors)
            if self.stop_export:
                message = "导出已停止\n" + message
            messagebox.showinfo("导出完成", message)
            self.update_status(message)
        else:
            messagebox.showwarning("警告", "没有找到可导出的聊天记录")
            self.update_status("导出失败: 没有可导出的聊天记录")

    def merge_records(self, selected=False):
        """合并聊天记录到单个文件"""
        # 获取文件ID列表
        if selected:
            file_ids = self.tree.selection()
            if not file_ids:
                messagebox.showinfo("提示", "没有选择任何文件")
                return
        else:
            file_ids = self.tree.get_children()
            if not file_ids:
                messagebox.showinfo("提示", "没有可合并的聊天记录")
                return

        # 弹出保存文件对话框
        output_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("文本文件", "*.txt"), ("所有文件", "*.*")],
            title="保存合并的聊天记录"
        )
        if not output_path:
            return

        # 处理文件冲突
        if os.path.exists(output_path):
            action = self.handle_file_conflict(output_path)
            if action == "skip":
                self.update_status("合并已取消")
                return
            elif action == "rename":
                base, ext = os.path.splitext(output_path)
                counter = 1
                while True:
                    new_path = f"{base}_{counter}{ext}"
                    if not os.path.exists(new_path):
                        output_path = new_path
                        break
                    counter += 1

        # 检查磁盘空间
        disk_usage = psutil.disk_usage(os.path.dirname(output_path) or '.')
        if disk_usage.free < 100 * 1024 * 1024:  # 少于100MB
            error_msg = f"磁盘空间不足: 仅剩 {disk_usage.free / 1024 / 1024:.2f} MB 可用空间"
            messagebox.showerror("合并错误", error_msg)
            self.update_status(error_msg)
            return

        # 创建进度窗口
        merge_win = tk.Toplevel(self.root)
        merge_win.title("合并进度")
        merge_win.geometry("400x200")
        merge_win.resizable(False, False)
        merge_win.grab_set()

        # 标题
        title_label = tk.Label(
            merge_win,
            text="正在合并聊天记录...",
            font=("Microsoft YaHei", 10, "bold"),
            pady=10
        )
        title_label.pack()

        # 进度标签
        progress_label = tk.Label(
            merge_win,
            text="准备中...",
            font=("Microsoft YaHei", 9)
        )
        progress_label.pack()

        # 进度条
        merge_progress = ttk.Progressbar(
            merge_win,
            orient=tk.HORIZONTAL,
            length=300,
            mode='determinate'
        )
        merge_progress.pack(pady=10)
        merge_progress["maximum"] = len(file_ids)

        # 停止按钮
        stop_btn = tk.Button(
            merge_win,
            text="停止合并",
            command=lambda: setattr(self, 'stop_merge', True),
            font=("Microsoft YaHei", 10),
            bg='#e74c3c',
            fg='white'
        )
        stop_btn.pack(pady=5)
        self.stop_merge = False  # 合并停止标志

        # 更新UI
        merge_win.update()

        try:
            total_records = 0
            merged_files = 0
            skipped_files = 0

            with open(output_path, 'w', encoding='utf-8') as outfile:
                for i, file_id in enumerate(file_ids):
                    if self.stop_merge:
                        break

                    records = self.results_by_id.get(file_id, [])

                    # 更新进度
                    merge_progress["value"] = i + 1

                    # 获取显示名称
                    values = self.tree.item(file_id, "values")
                    display_name = values[0]
                    progress_label.config(text=f"正在合并: {display_name} ({i + 1}/{len(file_ids)})")
                    merge_win.update()

                    if not records:
                        skipped_files += 1
                        continue

                    # 写入文件标题
                    outfile.write(f"===== {display_name} =====\n")
                    outfile.write("\n".join(records))
                    outfile.write("\n\n")  # 两个空行分隔

                    total_records += len(records)
                    merged_files += 1

                    # 短暂暂停，让UI更新
                    time.sleep(0.05)

            if self.stop_merge:
                message = f"合并已停止!\n已合并 {merged_files} 个文件\n总记录数: {total_records}"
            else:
                message = f"合并完成!\n共合并 {merged_files} 个文件（跳过 {skipped_files} 个无记录的文件）\n总记录数: {total_records}"
            messagebox.showinfo("合并完成", message)
            self.update_status(f"聊天记录已合并到: {output_path}")

        except Exception as e:
            messagebox.showerror("合并错误", f"合并过程中出错: {str(e)}")
            self.update_status(f"合并失败: {str(e)}")
        finally:
            merge_win.destroy()


def main():
    root = tk.Tk()
    app = ChatExtractorApp(root)

    # 启动工作线程
    app.worker_thread = threading.Thread(target=app.worker, daemon=True)
    app.worker_thread.start()

    root.mainloop()

    # 清理工作线程
    app.task_queue.put(None)
    app.worker_thread.join(timeout=1.0)

    # 清理临时目录
    app.cleanup_temp_dirs()


if __name__ == "__main__":
    main()