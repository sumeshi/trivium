"""Owned GTK3 file drag source, using only Python's standard library.

The separate process supplies a real Xdnd text/uri-list selection. The test
drives mouse input with XTest; this helper never calls the app's event API.
"""
from __future__ import annotations

import argparse
from ctypes import CDLL, CFUNCTYPE, POINTER, Structure, byref, c_char_p, c_double, c_int, c_uint, c_ulong, c_void_p, cast
import json
from pathlib import Path


class TargetEntry(Structure):
    _fields_ = [("target", c_char_p), ("flags", c_uint), ("info", c_uint)]


class Allocation(Structure):
    _fields_ = [("x", c_int), ("y", c_int), ("width", c_int), ("height", c_int)]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--x", type=int, required=True)
    parser.add_argument("--y", type=int, required=True)
    parser.add_argument("files", type=Path, nargs="+")
    args = parser.parse_args()
    for path in args.files:
        if not path.resolve().is_file():
            raise ValueError("The drag source requires an existing fixture file")

    gtk, gdk = CDLL("libgtk-3.so.0"), CDLL("libgdk-3.so.0")
    gobject, glib = CDLL("libgobject-2.0.so.0"), CDLL("libglib-2.0.so.0")

    def function(library, name, result, arguments):
        fn = getattr(library, name)
        fn.restype, fn.argtypes = result, arguments
        return fn

    init = function(gtk, "gtk_init_check", c_int, [c_void_p, c_void_p])
    if not init(None, None):
        raise RuntimeError("The owned GTK drag source requires the native test's X display")
    window = function(gtk, "gtk_window_new", c_void_p, [c_int])(0)
    button = function(gtk, "gtk_button_new_with_label", c_void_p, [c_char_p])(b"Drag test data files")
    function(gtk, "gtk_window_set_title", None, [c_void_p, c_char_p])(window, b"Trivium test file source")
    function(gtk, "gtk_window_set_decorated", None, [c_void_p, c_int])(window, 0)
    function(gtk, "gtk_window_set_resizable", None, [c_void_p, c_int])(window, 0)
    function(gtk, "gtk_window_set_keep_above", None, [c_void_p, c_int])(window, 1)
    function(gtk, "gtk_window_set_default_size", None, [c_void_p, c_int, c_int])(window, 220, 90)
    function(gtk, "gtk_window_move", None, [c_void_p, c_int, c_int])(window, args.x, args.y)
    function(gtk, "gtk_container_add", None, [c_void_p, c_void_p])(window, button)
    entries = (TargetEntry * 1)(TargetEntry(b"text/uri-list", 0, 0))
    function(gtk, "gtk_drag_source_set", None, [c_void_p, c_uint, POINTER(TargetEntry), c_int, c_uint])(
        button, 1 << 8, entries, 1, 1 << 1)  # Left button, copy only.
    uris = (c_char_p * (len(args.files) + 1))(
        *[path.resolve().as_uri().encode("utf-8") for path in args.files], None)
    set_uris = function(gtk, "gtk_selection_data_set_uris", c_int, [c_void_p, POINTER(c_char_p)])
    connect = function(gobject, "g_signal_connect_data", c_ulong,
                       [c_void_p, c_char_p, c_void_p, c_void_p, c_void_p, c_int])
    main_quit = function(gtk, "gtk_main_quit", None, [])

    def publish(event: str, **values) -> None:
        print(json.dumps({"event": event, **values}), flush=True)

    @CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)
    def drag_begin(_widget, _context, _data):
        publish("drag-begin")

    @CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)
    def drag_end(_widget, _context, _data):
        publish("drag-end")

    @CFUNCTYPE(None, c_void_p, c_void_p, c_void_p, c_uint, c_uint, c_void_p)
    def drag_data(_widget, _context, selection, _info, _time, _data):
        publish("selection-sent", count=len(args.files), accepted=bool(set_uris(selection, uris)))

    @CFUNCTYPE(None, c_void_p, c_void_p)
    def destroyed(_widget, _data):
        main_quit()

    event_type = function(gdk, "gdk_event_get_event_type", c_int, [c_void_p])
    event_coords = function(gdk, "gdk_event_get_coords", c_int, [c_void_p, POINTER(c_double), POINTER(c_double)])
    event_state = function(gdk, "gdk_event_get_state", c_int, [c_void_p, POINTER(c_uint)])

    @CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)
    def event_after(_widget, event, _data):
        kind = event_type(event)
        if kind in [3, 4, 7]:
            x, y, state = c_double(), c_double(), c_uint()
            event_coords(event, byref(x), byref(y))
            event_state(event, byref(state))
            publish("pointer", kind=kind, x=x.value, y=y.value, state=state.value)

    for signal, callback in [(b"drag-begin", drag_begin), (b"drag-end", drag_end),
                             (b"drag-data-get", drag_data), (b"destroy", destroyed), (b"event-after", event_after)]:
        connect(window if signal == b"destroy" else button, signal, cast(callback, c_void_p), None, None, 0)
    get_window = function(gtk, "gtk_widget_get_window", c_void_p, [c_void_p])
    get_origin = function(gdk, "gdk_window_get_origin", c_int, [c_void_p, POINTER(c_int), POINTER(c_int)])
    get_allocation = function(gtk, "gtk_widget_get_allocation", None, [c_void_p, POINTER(Allocation)])

    @CFUNCTYPE(c_int, c_void_p)
    def ready(_data):
        x, y, allocation = c_int(), c_int(), Allocation()
        get_origin(get_window(button), byref(x), byref(y))
        get_allocation(button, byref(allocation))
        publish("ready", x=x.value + allocation.x + allocation.width // 2,
                y=y.value + allocation.y + allocation.height // 2)
        return 0

    function(gtk, "gtk_widget_show_all", None, [c_void_p])(window)
    function(glib, "g_idle_add", c_uint, [type(ready), c_void_p])(ready, None)
    function(gtk, "gtk_main", None, [])()


if __name__ == "__main__":
    main()
