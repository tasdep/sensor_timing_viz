import argparse
import os
import signal
import sys
from pathlib import Path
from typing import Dict, List, Optional

from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg, NavigationToolbar2QT
from matplotlib.figure import Figure
from PyQt5.QtCore import QObject, Qt, QThread, QTimer, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .analysis import (
    analyze_bag,
    analyze_topic_data,
    build_timing_diagram_summary_table_rows,
    default_selected_topics,
    filter_analysis_result,
    parse_expected_periods,
    set_active_timing_basis,
)
from .args import add_bag_option, add_time_window_args
from .bag_io import discover_topics
from .models import AnalysisOptions, AnalysisResult, TopicData
from .plotting import render_bag_header_offset_figure, render_timing_diagram_figure, render_variability_figure
from .reporting import export_html_report


class TimingNavigationToolbar(NavigationToolbar2QT):
    def __init__(self, canvas, parent=None, blocked_axes_getter=None):
        super().__init__(canvas, parent)
        self._blocked_axes_getter = blocked_axes_getter
        self._blocked_zoom_gesture = False
        self._blocked_pan_gesture = False

    def _is_blocked_axis(self, event) -> bool:
        if self._blocked_axes_getter is None:
            return False
        if getattr(event, "inaxes", None) is None:
            return False
        return event.inaxes in set(self._blocked_axes_getter())

    def press_zoom(self, event):
        if self._is_blocked_axis(event):
            self._blocked_zoom_gesture = True
            return
        self._blocked_zoom_gesture = False
        return super().press_zoom(event)

    def drag_zoom(self, event):
        if self._blocked_zoom_gesture:
            return
        return super().drag_zoom(event)

    def release_zoom(self, event):
        if self._blocked_zoom_gesture:
            self._blocked_zoom_gesture = False
            return
        return super().release_zoom(event)

    def press_pan(self, event):
        if self._is_blocked_axis(event):
            self._blocked_pan_gesture = True
            return
        self._blocked_pan_gesture = False
        return super().press_pan(event)

    def drag_pan(self, event):
        if self._blocked_pan_gesture:
            return
        return super().drag_pan(event)

    def release_pan(self, event):
        if self._blocked_pan_gesture:
            self._blocked_pan_gesture = False
            return
        return super().release_pan(event)


class AnalysisWorker(QObject):
    finished = pyqtSignal(object, object)
    failed = pyqtSignal(str, object)

    def __init__(self, mode: str, payload):
        super().__init__()
        self.mode = mode
        self.payload = payload

    def run(self) -> None:
        try:
            if self.mode == "discover":
                bag_path, start_offset_s, end_offset_s = self.payload
                result = discover_topics(bag_path, start_offset_s=start_offset_s, end_offset_s=end_offset_s)
            elif self.mode == "analyze":
                options, preloaded_topic_data = self.payload
                if preloaded_topic_data is None:
                    result = analyze_bag(options)
                else:
                    result = analyze_topic_data(options, preloaded_topic_data)
            else:
                raise RuntimeError(f"Unknown worker mode: {self.mode}")
            self.finished.emit(result, self.payload)
        except Exception as error:
            self.failed.emit(str(error), self.payload)


class TimingViewerWindow(QMainWindow):
    def __init__(
        self,
        initial_bag: Optional[Path] = None,
        initial_start_offset_s: float = 0.0,
        initial_end_offset_s: Optional[float] = None,
    ):
        super().__init__()
        self.setWindowTitle("Sensor Timing Viewer")
        self.resize(1600, 950)

        self.current_topics: Dict[str, TopicData] = {}
        self.available_plot_topics: List[str] = []
        self.current_result: Optional[AnalysisResult] = None
        self.full_result: Optional[AnalysisResult] = None
        self.last_analysis_signature = None
        self.quit_after_load_requested = False
        self.render_after_worker_cleanup = False
        self.worker_thread: Optional[QThread] = None
        self.worker: Optional[AnalysisWorker] = None
        self.pending_visible_topics: List[str] = []
        self._rendering_topics = False
        self._render_timer = QTimer(self)
        self._render_timer.setSingleShot(True)
        self._render_timer.setInterval(150)
        self._render_timer.timeout.connect(self.render_current_selection)
        self.figure = Figure(figsize=(12, 8), constrained_layout=True)
        self.canvas = FigureCanvasQTAgg(self.figure)
        self.toolbar = TimingNavigationToolbar(self.canvas, self, blocked_axes_getter=self.blocked_timing_toolbar_axes)
        self.bag_header_offset_figure = Figure(figsize=(12, 8), constrained_layout=True)
        self.bag_header_offset_canvas = FigureCanvasQTAgg(self.bag_header_offset_figure)
        self.bag_header_offset_toolbar = NavigationToolbar2QT(self.bag_header_offset_canvas, self)
        self.variability_figure = Figure(figsize=(12, 8), constrained_layout=True)
        self.variability_canvas = FigureCanvasQTAgg(self.variability_figure)
        self.variability_toolbar = NavigationToolbar2QT(self.variability_canvas, self)
        self.main_axis = None
        self.overview_axis = None
        self.main_full_ylim = None
        self.main_full_xlim = None
        self.hover_line: Optional[Line2D] = None
        self.overview_window_patch: Optional[Rectangle] = None
        self.overview_left_handle: Optional[Rectangle] = None
        self.overview_right_handle: Optional[Rectangle] = None
        self._overview_drag_mode: Optional[str] = None
        self._overview_drag_offset = 0.0
        self._overview_drag_start_xlim = None
        self._canvas_connections: List[int] = []

        self.bag_path_edit = QLineEdit(str(initial_bag) if initial_bag else "")
        self.title_edit = QLineEdit()
        self.timestamp_combo = QComboBox()
        self.timestamp_combo.addItems(["header", "bag"])
        self.timestamp_combo.setCurrentText("header")
        self.timestamp_combo.currentTextChanged.connect(self.schedule_render)

        self.start_time_spin = QDoubleSpinBox()
        self.start_time_spin.setRange(0.0, 1_000_000.0)
        self.start_time_spin.setValue(max(0.0, initial_start_offset_s))
        self.start_time_spin.setSingleStep(1.0)

        self.end_time_spin = QDoubleSpinBox()
        self.end_time_spin.setRange(0.0, 1_000_000.0)
        self.end_time_spin.setValue(0.0 if initial_end_offset_s is None else max(0.0, initial_end_offset_s))
        self.end_time_spin.setSingleStep(1.0)
        self.end_time_spin.setSpecialValueText("Bag End")

        self.gap_factor_spin = QDoubleSpinBox()
        self.gap_factor_spin.setRange(1.0, 100.0)
        self.gap_factor_spin.setValue(3.0)
        self.gap_factor_spin.setSingleStep(0.5)

        self.min_gap_checkbox = QCheckBox("Use minimum gap threshold [s]")
        self.min_gap_spin = QDoubleSpinBox()
        self.min_gap_spin.setRange(0.0, 1000.0)
        self.min_gap_spin.setValue(0.0)
        self.min_gap_spin.setSingleStep(0.05)
        self.min_gap_spin.setEnabled(False)
        self.min_gap_checkbox.toggled.connect(self.min_gap_spin.setEnabled)

        self.width_spin = QDoubleSpinBox()
        self.width_spin.setRange(4.0, 60.0)
        self.width_spin.setValue(16.0)
        self.width_spin.setSingleStep(1.0)

        self.height_spin = QDoubleSpinBox()
        self.height_spin.setRange(0.0, 60.0)
        self.height_spin.setValue(0.0)
        self.height_spin.setSingleStep(1.0)
        self.height_spin.setSpecialValueText("Auto")

        self.dpi_spin = QDoubleSpinBox()
        self.dpi_spin.setRange(50.0, 600.0)
        self.dpi_spin.setValue(150.0)
        self.dpi_spin.setSingleStep(10.0)

        self.expected_periods_edit = QTextEdit()
        self.expected_periods_edit.setPlaceholderText("/imu/data=0.02\n/livox/lidar=0.1")
        self.expected_periods_edit.setMaximumHeight(90)

        self.topic_list = QListWidget()
        self.topic_list.setSelectionMode(QListWidget.NoSelection)
        self.topic_list.itemChanged.connect(self.on_topic_item_changed)

        self.summary_table = QTableWidget()
        self.summary_table.setColumnCount(8)
        self.summary_table.setHorizontalHeaderLabels(
            [
                "Topic",
                "Count",
                "Median dt [ms]",
                "Effective Rate [Hz]",
                "Max gap [s]",
                "Gap count",
                "Threshold [s]",
                "Gap windows [s]",
            ]
        )
        self.summary_table.horizontalHeader().setStretchLastSection(True)
        self.timing_variability_topic_combo = QComboBox()
        self.timing_variability_topic_combo.currentTextChanged.connect(self.render_timing_variability_view)
        self.timing_variability_basis_combo = QComboBox()
        self.timing_variability_basis_combo.addItems(["bag", "header"])
        self.timing_variability_basis_combo.setCurrentText("header")
        self.timing_variability_basis_combo.currentTextChanged.connect(self.render_timing_variability_view)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setVisible(False)
        self.tab_widget = QTabWidget()

        self._build_ui()

    def blocked_timing_toolbar_axes(self):
        return [axis for axis in [self.overview_axis] if axis is not None]

    def _build_ui(self) -> None:
        controls = QWidget()
        controls_layout = QVBoxLayout(controls)

        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Bag"))
        path_layout.addWidget(self.bag_path_edit, stretch=1)

        browse_button = QPushButton("Browse")
        browse_button.clicked.connect(self.choose_bag_path)
        path_layout.addWidget(browse_button)

        load_button = QPushButton("Load Topics")
        load_button.clicked.connect(self.load_topics)
        path_layout.addWidget(load_button)
        controls_layout.addLayout(path_layout)

        grid = QGridLayout()
        grid.addWidget(QLabel("Title"), 0, 0)
        grid.addWidget(self.title_edit, 0, 1, 1, 3)
        grid.addWidget(QLabel("Gap Factor"), 1, 0)
        grid.addWidget(self.gap_factor_spin, 1, 1)
        grid.addWidget(QLabel("Start [s]"), 1, 2)
        grid.addWidget(self.start_time_spin, 1, 3)
        grid.addWidget(QLabel("End [s]"), 2, 0)
        grid.addWidget(self.end_time_spin, 2, 1)
        grid.addWidget(self.min_gap_checkbox, 2, 2)
        grid.addWidget(self.min_gap_spin, 2, 3)
        grid.addWidget(QLabel("Fig Width"), 3, 0)
        grid.addWidget(self.width_spin, 3, 1)
        grid.addWidget(QLabel("Fig Height"), 3, 2)
        grid.addWidget(self.height_spin, 3, 3)
        grid.addWidget(QLabel("DPI"), 4, 0)
        grid.addWidget(self.dpi_spin, 4, 1)
        controls_layout.addLayout(grid)

        controls_layout.addWidget(QLabel("Expected Period Overrides"))
        controls_layout.addWidget(self.expected_periods_edit)

        topic_buttons = QHBoxLayout()
        select_all_button = QPushButton("Select All")
        select_all_button.clicked.connect(lambda: self.set_all_topics_checked(True))
        clear_button = QPushButton("Clear")
        clear_button.clicked.connect(lambda: self.set_all_topics_checked(False))
        render_button = QPushButton("Render")
        render_button.clicked.connect(self.render_current_selection)
        save_button = QPushButton("Save Image")
        save_button.clicked.connect(self.save_image)
        report_button = QPushButton("Export Report")
        report_button.clicked.connect(self.export_html_report_file)
        reset_button = QPushButton("Reset View")
        reset_button.clicked.connect(self.reset_view)

        topic_buttons.addWidget(select_all_button)
        topic_buttons.addWidget(clear_button)
        topic_buttons.addWidget(render_button)
        topic_buttons.addWidget(save_button)
        topic_buttons.addWidget(report_button)
        topic_buttons.addWidget(reset_button)
        controls_layout.addLayout(topic_buttons)
        controls_layout.addWidget(self.progress_bar)

        controls_layout.addWidget(QLabel("Topics"))
        controls_layout.addWidget(self.topic_list, stretch=1)

        timing_tab = QWidget()
        timing_layout = QVBoxLayout(timing_tab)
        timing_layout.addWidget(self.toolbar)
        timing_layout.addWidget(self.canvas, stretch=2)
        summary_header = QHBoxLayout()
        summary_header.addWidget(QLabel("Summary"))
        summary_header.addStretch(1)
        summary_header.addWidget(QLabel("Timing Basis"))
        summary_header.addWidget(self.timestamp_combo)
        timing_layout.addLayout(summary_header)
        timing_layout.addWidget(self.summary_table, stretch=1)

        bag_header_offset_tab = QWidget()
        bag_header_offset_layout = QVBoxLayout(bag_header_offset_tab)
        bag_header_offset_layout.addWidget(self.bag_header_offset_toolbar)
        bag_header_offset_layout.addWidget(self.bag_header_offset_canvas, stretch=1)

        timing_variability_tab = QWidget()
        timing_variability_layout = QVBoxLayout(timing_variability_tab)
        timing_variability_header = QHBoxLayout()
        timing_variability_header.addWidget(QLabel("Topic"))
        timing_variability_header.addWidget(self.timing_variability_topic_combo, stretch=1)
        timing_variability_header.addWidget(QLabel("Basis"))
        timing_variability_header.addWidget(self.timing_variability_basis_combo)
        timing_variability_layout.addLayout(timing_variability_header)
        timing_variability_layout.addWidget(self.variability_toolbar)
        timing_variability_layout.addWidget(self.variability_canvas, stretch=1)

        self.tab_widget.addTab(timing_tab, "Timing Diagram")
        self.tab_widget.addTab(bag_header_offset_tab, "Bag-Header Offset")
        self.tab_widget.addTab(timing_variability_tab, "Timing Variability")

        splitter = QSplitter()
        splitter.addWidget(controls)
        splitter.addWidget(self.tab_widget)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        self.setCentralWidget(splitter)

    def choose_bag_path(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Select rosbag2 directory", self.bag_path_edit.text() or ".")
        if path:
            self.bag_path_edit.setText(path)

    def set_all_topics_checked(self, checked: bool) -> None:
        self._rendering_topics = True
        for index in range(self.topic_list.count()):
            item = self.topic_list.item(index)
            item.setCheckState(Qt.Checked if checked else Qt.Unchecked)
        self._rendering_topics = False
        self.schedule_render()

    def selected_topics(self) -> List[str]:
        topics: List[str] = []
        for index in range(self.topic_list.count()):
            item = self.topic_list.item(index)
            if item.checkState() == Qt.Checked:
                topics.append(item.text())
        return topics

    def minimum_gap_threshold(self) -> Optional[float]:
        return self.min_gap_spin.value() if self.min_gap_checkbox.isChecked() else None

    def clear_rendered_views(self) -> None:
        self.figure.clear()
        self.bag_header_offset_figure.clear()
        self.variability_figure.clear()
        self.canvas.draw()
        self.bag_header_offset_canvas.draw()
        self.variability_canvas.draw()
        self.summary_table.setRowCount(0)

    def current_analysis_signature(self, options: AnalysisOptions) -> tuple:
        return (
            str(options.bag_path),
            tuple(options.selected_topics or []),
            options.start_offset_s,
            options.end_offset_s,
            options.gap_threshold_factor,
            options.gap_threshold_sec,
            tuple(sorted(options.expected_periods.items())),
            options.title,
            options.figure_width,
            options.figure_height,
            options.dpi,
        )

    def populate_topic_list(self, topic_names: List[str]) -> None:
        defaults = set(topic_names)
        self._rendering_topics = True
        self.topic_list.clear()
        for topic in topic_names:
            item = QListWidgetItem(topic)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if topic in defaults else Qt.Unchecked)
            self.topic_list.addItem(item)
        self._rendering_topics = False

    def load_topics(self) -> None:
        bag_text = self.bag_path_edit.text().strip()
        if not bag_text:
            self.show_error("Choose a bag path first.")
            return
        bag_path = Path(bag_text).expanduser()
        payload = (bag_path, self.start_time_spin.value(), self.selected_end_offset())
        self.start_worker("discover", payload, "Loading topics...")

    def expected_period_overrides(self) -> Dict[str, float]:
        lines = [line.strip() for line in self.expected_periods_edit.toPlainText().splitlines() if line.strip()]
        return parse_expected_periods(lines)

    def current_options(self) -> AnalysisOptions:
        figure_height = self.height_spin.value()
        return AnalysisOptions(
            bag_path=Path(self.bag_path_edit.text()).expanduser(),
            selected_topics=list(self.available_plot_topics),
            timestamp_source=self.timestamp_combo.currentText(),
            start_offset_s=self.start_time_spin.value(),
            end_offset_s=self.selected_end_offset(),
            gap_threshold_factor=self.gap_factor_spin.value(),
            gap_threshold_sec=self.minimum_gap_threshold(),
            expected_periods=self.expected_period_overrides(),
            title=self.title_edit.text() or None,
            figure_width=self.width_spin.value(),
            figure_height=figure_height if figure_height > 0.0 else None,
            dpi=int(self.dpi_spin.value()),
        )

    def render_current_selection(self) -> None:
        try:
            visible_topics = self.selected_topics()
            if not visible_topics:
                self.current_result = None
                self.clear_rendered_views()
                self.statusBar().showMessage("Select at least one topic to render.", 4000)
                return

            options = self.current_options()
            analysis_signature = self.current_analysis_signature(options)
            if self.full_result is None or self.last_analysis_signature != analysis_signature:
                self.pending_visible_topics = visible_topics
                preloaded_topic_data = self.current_topics if self.current_topics else None
                self.start_worker("analyze", (options, preloaded_topic_data), "Analyzing bag...")
                return

            self.current_result = filter_analysis_result(self.full_result, visible_topics)
            self.update_rendered_views()
        except KeyboardInterrupt:
            QApplication.instance().quit()
            return
        except Exception as error:
            self.show_error(str(error))

    def update_rendered_views(self) -> None:
        if self.current_result is None:
            return
        preserved_timing_xlim = self.main_axis.get_xlim() if self.main_axis is not None else None
        set_active_timing_basis(self.current_result, self.timestamp_combo.currentText())
        render_timing_diagram_figure(self.current_result, self.figure, embedded=True)
        self.bind_timing_interactions()
        if preserved_timing_xlim is not None and self.main_axis is not None:
            self.main_axis.set_xlim(*preserved_timing_xlim)
            self.sync_main_axis_limits()
        self.canvas.draw()
        self.render_bag_header_offset_view()
        self.populate_timing_variability_topics()
        self.render_timing_variability_view()
        self.populate_timing_diagram_summary_table()
        self.statusBar().showMessage("Timing diagram updated", 4000)

    def populate_timing_diagram_summary_table(self) -> None:
        if self.current_result is None:
            self.summary_table.setRowCount(0)
            return
        rows = build_timing_diagram_summary_table_rows(self.current_result.summaries, self.current_result.gap_map)
        self.summary_table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            for column_index, value in enumerate(row):
                self.summary_table.setItem(row_index, column_index, QTableWidgetItem(value))
        self.summary_table.resizeColumnsToContents()

    def render_bag_header_offset_view(self) -> None:
        if self.current_result is None:
            self.bag_header_offset_figure.clear()
            self.bag_header_offset_canvas.draw()
            return
        render_bag_header_offset_figure(self.current_result, self.bag_header_offset_figure, embedded=True)
        self.bag_header_offset_canvas.draw()

    def populate_timing_variability_topics(self) -> None:
        current_text = self.timing_variability_topic_combo.currentText()
        self.timing_variability_topic_combo.blockSignals(True)
        self.timing_variability_topic_combo.clear()
        if self.current_result is not None:
            self.timing_variability_topic_combo.addItems(self.current_result.topic_names)
            if current_text and current_text in self.current_result.topic_names:
                self.timing_variability_topic_combo.setCurrentText(current_text)
        self.timing_variability_topic_combo.blockSignals(False)

    def render_timing_variability_view(self) -> None:
        if self.current_result is None or not self.current_result.topic_names:
            self.variability_figure.clear()
            self.variability_canvas.draw()
            return
        topic_name = self.timing_variability_topic_combo.currentText() or self.current_result.topic_names[0]
        if topic_name not in self.current_result.topic_names:
            topic_name = self.current_result.topic_names[0]
            self.timing_variability_topic_combo.blockSignals(True)
            self.timing_variability_topic_combo.setCurrentText(topic_name)
            self.timing_variability_topic_combo.blockSignals(False)
        render_variability_figure(
            self.current_result,
            topic_name,
            self.variability_figure,
            embedded=True,
            timestamp_basis=self.timing_variability_basis_combo.currentText(),
        )
        self.variability_canvas.draw()

    def on_topic_item_changed(self, _item: QListWidgetItem) -> None:
        if self._rendering_topics:
            return
        self.schedule_render()

    def schedule_render(self) -> None:
        self._render_timer.start()

    def bind_timing_interactions(self) -> None:
        self.disconnect_timing_interactions()
        self.main_axis = None
        self.overview_axis = None
        for axis in self.figure.axes:
            if axis.get_gid() == "timing_main_axis":
                self.main_axis = axis
            elif axis.get_gid() == "timing_overview_axis":
                self.overview_axis = axis

        if self.main_axis is None or self.overview_axis is None:
            return

        self.main_full_ylim = self.main_axis.get_ylim()
        self.main_full_xlim = self.main_axis.get_xlim()
        self.hover_line = self.main_axis.axvline(
            self.main_full_xlim[0],
            color="#444444",
            linewidth=1.0,
            linestyle="--",
            alpha=0.8,
            visible=False,
            zorder=10,
        )
        overview_bottom, overview_top = self.overview_axis.get_ylim()
        self.overview_window_patch = Rectangle(
            (self.main_full_xlim[0], overview_bottom),
            self.main_full_xlim[1] - self.main_full_xlim[0],
            overview_top - overview_bottom,
            facecolor="#4c78a8",
            edgecolor="#2f4b7c",
            alpha=0.18,
            linewidth=1.0,
            zorder=5,
        )
        self.overview_axis.add_patch(self.overview_window_patch)
        handle_width = self.overview_handle_width()
        self.overview_left_handle = Rectangle(
            (self.main_full_xlim[0] - handle_width / 2.0, overview_bottom),
            handle_width,
            overview_top - overview_bottom,
            facecolor="#2f4b7c",
            edgecolor="#1f3558",
            alpha=0.7,
            linewidth=1.0,
            zorder=6,
        )
        self.overview_right_handle = Rectangle(
            (self.main_full_xlim[1] - handle_width / 2.0, overview_bottom),
            handle_width,
            overview_top - overview_bottom,
            facecolor="#2f4b7c",
            edgecolor="#1f3558",
            alpha=0.7,
            linewidth=1.0,
            zorder=6,
        )
        self.overview_axis.add_patch(self.overview_left_handle)
        self.overview_axis.add_patch(self.overview_right_handle)
        self.sync_main_axis_limits()

        self._canvas_connections = [
            self.canvas.mpl_connect("motion_notify_event", self.on_canvas_mouse_move),
            self.canvas.mpl_connect("axes_leave_event", self.on_axes_leave),
            self.canvas.mpl_connect("button_release_event", self.on_main_plot_release),
            self.canvas.mpl_connect("scroll_event", self.on_main_plot_release),
            self.canvas.mpl_connect("button_press_event", self.on_overview_click),
        ]
        self.main_axis.callbacks.connect("xlim_changed", lambda _ax: self.update_overview_window())

    def disconnect_timing_interactions(self) -> None:
        for connection_id in self._canvas_connections:
            self.canvas.mpl_disconnect(connection_id)
        self._canvas_connections = []
        self._overview_drag_mode = None
        self._overview_drag_start_xlim = None

    def sync_main_axis_limits(self) -> None:
        if self.main_axis is None or self.main_full_ylim is None or self.main_full_xlim is None:
            return
        x0, x1 = self.main_axis.get_xlim()
        full_x0, full_x1 = self.main_full_xlim
        clamped_x0 = max(full_x0, min(x0, full_x1))
        clamped_x1 = max(clamped_x0 + 1e-6, min(x1, full_x1))
        if clamped_x0 != x0 or clamped_x1 != x1:
            self.main_axis.set_xlim(clamped_x0, clamped_x1)
        self.main_axis.set_ylim(*self.main_full_ylim)
        self.update_overview_window()

    def overview_handle_width(self) -> float:
        if self.main_full_xlim is None:
            return 0.1
        full_x0, full_x1 = self.main_full_xlim
        return max((full_x1 - full_x0) * 0.01, 0.05)

    def update_overview_window(self) -> None:
        if (
            self.main_axis is None
            or self.overview_axis is None
            or self.overview_window_patch is None
            or self.overview_left_handle is None
            or self.overview_right_handle is None
        ):
            return
        x0, x1 = self.main_axis.get_xlim()
        bottom, top = self.overview_axis.get_ylim()
        handle_width = self.overview_handle_width()
        self.overview_window_patch.set_x(x0)
        self.overview_window_patch.set_width(max(0.0, x1 - x0))
        self.overview_window_patch.set_y(bottom)
        self.overview_window_patch.set_height(top - bottom)
        self.overview_left_handle.set_x(x0 - handle_width / 2.0)
        self.overview_left_handle.set_y(bottom)
        self.overview_left_handle.set_width(handle_width)
        self.overview_left_handle.set_height(top - bottom)
        self.overview_right_handle.set_x(x1 - handle_width / 2.0)
        self.overview_right_handle.set_y(bottom)
        self.overview_right_handle.set_width(handle_width)
        self.overview_right_handle.set_height(top - bottom)

    def on_canvas_mouse_move(self, event) -> None:
        if self._overview_drag_mode is not None:
            self.on_overview_drag(event)
        self.update_main_plot_hover(event)

    def update_main_plot_hover(self, event) -> None:
        if self.main_axis is None or self.hover_line is None:
            return
        if event.inaxes != self.main_axis or event.xdata is None:
            if self.hover_line.get_visible():
                self.hover_line.set_visible(False)
                self.canvas.draw_idle()
            return
        self.hover_line.set_xdata([event.xdata, event.xdata])
        self.hover_line.set_visible(True)
        self.statusBar().showMessage(f"Time: {event.xdata:.3f} s", 1000)
        self.canvas.draw_idle()

    def on_axes_leave(self, event) -> None:
        if event.inaxes == self.main_axis and self.hover_line is not None and self.hover_line.get_visible():
            self.hover_line.set_visible(False)
            self.canvas.draw_idle()

    def on_main_plot_release(self, _event) -> None:
        self._overview_drag_mode = None
        self._overview_drag_start_xlim = None
        self.sync_main_axis_limits()
        self.canvas.draw_idle()

    def on_overview_click(self, event) -> None:
        if event.inaxes != self.overview_axis or event.xdata is None or self.main_axis is None:
            return
        if getattr(event, "dblclick", False) and event.button == 1:
            if self.main_full_xlim is not None:
                self.main_axis.set_xlim(*self.main_full_xlim)
                self.sync_main_axis_limits()
                self.canvas.draw_idle()
            return
        if event.button == 1:
            x0, x1 = self.main_axis.get_xlim()
            handle_half = self.overview_handle_width() / 2.0
            if abs(event.xdata - x0) <= handle_half:
                self._overview_drag_mode = "left"
                self._overview_drag_start_xlim = (x0, x1)
                return
            if abs(event.xdata - x1) <= handle_half:
                self._overview_drag_mode = "right"
                self._overview_drag_start_xlim = (x0, x1)
                return
            if self.overview_window_patch is not None:
                patch_x = self.overview_window_patch.get_x()
                patch_width = self.overview_window_patch.get_width()
                if patch_x <= event.xdata <= patch_x + patch_width:
                    self._overview_drag_mode = "move"
                    self._overview_drag_offset = event.xdata - patch_x
                    self._overview_drag_start_xlim = (x0, x1)
                    return
            width = x1 - x0
            self.main_axis.set_xlim(event.xdata - width / 2.0, event.xdata + width / 2.0)
            self.sync_main_axis_limits()
            self.canvas.draw_idle()
            return
        if event.button not in (2, 3):
            return
        x0, x1 = self.main_axis.get_xlim()
        width = x1 - x0
        self.main_axis.set_xlim(event.xdata - width / 2.0, event.xdata + width / 2.0)
        self.sync_main_axis_limits()
        self.canvas.draw_idle()

    def on_overview_drag(self, event) -> None:
        if (
            self._overview_drag_mode is None
            or event.inaxes != self.overview_axis
            or event.xdata is None
            or self.main_axis is None
            or self.main_full_xlim is None
            or self._overview_drag_start_xlim is None
        ):
            return
        x0, x1 = self._overview_drag_start_xlim
        width = x1 - x0
        full_x0, full_x1 = self.main_full_xlim
        min_width = max((full_x1 - full_x0) * 0.002, 1e-4)

        if self._overview_drag_mode == "move":
            new_x0 = event.xdata - self._overview_drag_offset
            new_x0 = max(full_x0, min(new_x0, full_x1 - width))
            new_x1 = new_x0 + width
        elif self._overview_drag_mode == "left":
            new_x0 = max(full_x0, min(event.xdata, x1 - min_width))
            new_x1 = x1
        elif self._overview_drag_mode == "right":
            new_x0 = x0
            new_x1 = min(full_x1, max(event.xdata, x0 + min_width))
        else:
            return
        self.main_axis.set_xlim(new_x0, new_x1)
        self.sync_main_axis_limits()
        self.canvas.draw_idle()

    def save_image(self) -> None:
        if self.current_result is None:
            self.show_error("Render a timing diagram before saving.")
            return

        bag_label = self.current_result.bag_label
        default_name = f"{bag_label}_timing_diagram.png"
        if self.tab_widget.currentIndex() == 1:
            default_name = f"{bag_label}_bag_header_offset.png"
        elif self.tab_widget.currentIndex() == 2:
            topic_name = self.timing_variability_topic_combo.currentText() or "topic"
            topic_slug = topic_name.strip("/").replace("/", "__") or "topic"
            basis = self.timing_variability_basis_combo.currentText()
            default_name = f"{bag_label}_timing_variability_{topic_slug}_{basis}.png"

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save image",
            default_name,
            "PNG Files (*.png);;PDF Files (*.pdf);;All Files (*)",
        )
        if not path:
            return
        if self.tab_widget.currentIndex() == 0:
            self.figure.savefig(path)
        elif self.tab_widget.currentIndex() == 1:
            self.bag_header_offset_figure.savefig(path)
        else:
            self.variability_figure.savefig(path)
        self.statusBar().showMessage(f"Saved figure to {path}", 5000)

    def export_html_report_file(self) -> None:
        if self.current_result is None:
            self.show_error("Render an analysis before exporting a report.")
            return
        output_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save HTML report",
            str((Path(self.bag_path_edit.text()).expanduser().parent if self.bag_path_edit.text() else Path(".")) / "sensor_timing_report.html"),
            "HTML Files (*.html);;All Files (*)",
        )
        if not output_path:
            return
        export_html_report(
            self.current_result,
            Path(output_path),
            timing_diagram_summary_basis=self.timestamp_combo.currentText(),
            variability_basis=self.timing_variability_basis_combo.currentText(),
        )
        self.statusBar().showMessage(f"Exported HTML report to {output_path}", 5000)

    def reset_view(self) -> None:
        if self.tab_widget.currentIndex() == 0:
            self.toolbar.home()
        else:
            self.bag_header_offset_toolbar.home()

    def show_error(self, message: str) -> None:
        QMessageBox.critical(self, "Sensor Timing Viewer", message)

    def selected_end_offset(self) -> Optional[float]:
        value = self.end_time_spin.value()
        return None if value <= 0.0 else value

    def closeEvent(self, event) -> None:
        if self.worker_thread is not None and self.worker_thread.isRunning():
            self.worker_thread.quit()
            if not self.worker_thread.wait(1000):
                self.worker_thread.terminate()
                self.worker_thread.wait(1000)
        event.accept()

    def start_worker(self, mode: str, payload, status_message: str) -> None:
        if self.worker_thread is not None and self.worker_thread.isRunning():
            self.statusBar().showMessage("Background task already running...", 3000)
            return
        self.progress_bar.setVisible(True)
        self.statusBar().showMessage(status_message)
        self.worker_thread = QThread(self)
        self.worker = AnalysisWorker(mode, payload)
        self.worker.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_worker_finished)
        self.worker.failed.connect(self.on_worker_failed)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self.cleanup_worker)
        self.worker_thread.start()

    def on_worker_finished(self, result, payload) -> None:
        self.progress_bar.setVisible(False)
        if isinstance(payload, tuple) and payload and isinstance(payload[0], Path):
            bag_path, _, _ = payload
            self.current_topics = result
            self.available_plot_topics = default_selected_topics(self.current_topics)
            self.full_result = None
            self.current_result = None
            self.last_analysis_signature = None
            self.populate_topic_list(self.available_plot_topics)
            self.statusBar().showMessage(f"Loaded {len(self.current_topics)} topics from {bag_path}", 5000)
            self.render_after_worker_cleanup = True
            return

        self.full_result = result
        options, _preloaded_topic_data = payload
        self.last_analysis_signature = self.current_analysis_signature(options)
        visible_topics = self.pending_visible_topics or self.selected_topics()
        self.current_result = filter_analysis_result(self.full_result, visible_topics)
        self.update_rendered_views()
        if self.quit_after_load_requested:
            QTimer.singleShot(0, QApplication.instance().quit)

    def on_worker_failed(self, message: str, _payload) -> None:
        self.progress_bar.setVisible(False)
        self.show_error(message)
        if self.quit_after_load_requested:
            QTimer.singleShot(0, QApplication.instance().quit)

    def cleanup_worker(self) -> None:
        self.worker = None
        self.worker_thread = None
        if self.render_after_worker_cleanup:
            self.render_after_worker_cleanup = False
            QTimer.singleShot(0, self.render_current_selection)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch the interactive sensor timing GUI.")
    add_bag_option(parser, required=False)
    add_time_window_args(parser)
    parser.add_argument(
        "--quit-after-load",
        action="store_true",
        help="Load the bag, render once, and quit. Useful for smoke tests in headless environments.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = QApplication(sys.argv)

    def handle_sigint(*_args) -> None:
        app.closeAllWindows()
        app.quit()
        QTimer.singleShot(500, lambda: os._exit(130))

    signal.signal(signal.SIGINT, handle_sigint)

    # Give Python regular chances to process SIGINT while Qt owns the event loop.
    signal_timer = QTimer()
    signal_timer.setInterval(200)
    signal_timer.timeout.connect(lambda: None)
    signal_timer.start()

    try:
        window = TimingViewerWindow(
            initial_bag=args.bag,
            initial_start_offset_s=args.start,
            initial_end_offset_s=args.end,
        )
        window.show()

        if args.bag:
            if args.quit_after_load:
                window.quit_after_load_requested = True
            QTimer.singleShot(0, window.load_topics)
        elif args.quit_after_load:
            QTimer.singleShot(1500, app.quit)

        sys.exit(app.exec_())
    except KeyboardInterrupt:
        app.quit()
        sys.exit(130)


if __name__ == "__main__":
    main()
