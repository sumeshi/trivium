"""Choose an owned GTK file dialog with real X11 keys, never a scope grant."""
from __future__ import annotations

import json
import os
from pathlib import Path
import time

from row_navigation import wait_for


OPEN_TITLE = "Trivium native permission open"
SAVE_TITLE = "Trivium native permission save"


class _OwnedDialog:
    def __init__(self, main: int, pid: int, title: str):
        # Keeping X11 imports here lets Windows import the enclosing test suite.
        from ctypes import (CDLL, POINTER, Structure, c_char_p, c_int, c_long,
                            c_ubyte, c_uint, c_ulong, c_void_p)
        from runtime import _x11_open

        class Attributes(Structure):
            _fields_ = [(name, kind) for name, kind in (
                ("x", c_int), ("y", c_int), ("width", c_int), ("height", c_int),
                ("border_width", c_int), ("depth", c_int), ("visual", c_void_p),
                ("root", c_ulong), ("window_class", c_int), ("bit_gravity", c_int),
                ("win_gravity", c_int), ("backing_store", c_int), ("backing_planes", c_ulong),
                ("backing_pixel", c_ulong), ("save_under", c_int), ("colormap", c_ulong),
                ("map_installed", c_int), ("map_state", c_int), ("all_event_masks", c_long),
                ("your_event_mask", c_long), ("do_not_propagate_mask", c_long),
                ("override_redirect", c_int), ("screen", c_void_p))]

        self.main, self.pid, self.title = main, pid, title.encode("ascii")
        self.x11, self.display = _x11_open()
        self.xtest = CDLL("libXtst.so.6")
        self.Attributes = Attributes
        signatures = {
            "XFetchName": ([c_void_p, c_ulong, POINTER(c_void_p)], c_int),
            "XGetTransientForHint": ([c_void_p, c_ulong, POINTER(c_ulong)], c_int),
            "XGetInputFocus": ([c_void_p, POINTER(c_ulong), POINTER(c_int)], c_int),
            "XSetInputFocus": ([c_void_p, c_ulong, c_int, c_ulong], c_int),
            "XRaiseWindow": ([c_void_p, c_ulong], c_int),
            "XGetWindowAttributes": ([c_void_p, c_ulong, POINTER(Attributes)], c_int),
            "XKeysymToKeycode": ([c_void_p, c_ulong], c_ubyte),
            "XkbKeycodeToKeysym": ([c_void_p, c_ubyte, c_int, c_int], c_ulong),
            "XGetWindowProperty": ([c_void_p, c_ulong, c_ulong, c_long, c_long, c_int,
                c_ulong, POINTER(c_ulong), POINTER(c_int), POINTER(c_ulong), POINTER(c_ulong),
                POINTER(POINTER(c_ubyte))], c_int),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.x11, name)
            function.argtypes, function.restype = arguments, result
        self.xtest.XTestFakeKeyEvent.argtypes = [c_void_p, c_uint, c_int, c_ulong]
        self.xtest.XTestFakeKeyEvent.restype = c_int
        self.pid_atom = self.x11.XInternAtom(self.display, b"_NET_WM_PID", 1)
        assert self.pid_atom, "The owned GTK process must identify its windows"
        self.window = None
        self.discovery = {}

    def close(self):
        self.x11.XCloseDisplay(self.display)

    def _pid(self, window: int) -> int | None:
        from ctypes import POINTER, byref, c_int, c_ubyte, c_ulong, cast
        data, actual, kind, count, remaining = POINTER(c_ubyte)(), c_ulong(), c_int(), c_ulong(), c_ulong()
        try:
            status = self.x11.XGetWindowProperty(self.display, window, self.pid_atom, 0, 1, 0, 0,
                byref(actual), byref(kind), byref(count), byref(remaining), byref(data))
            return int(cast(data, POINTER(c_ulong))[0]) if status == 0 and kind.value == 32 and count.value == 1 and data else None
        finally:
            if data:
                self.x11.XFree(data)

    def _valid_parent(self, window: int) -> bool:
        from ctypes import byref, c_ulong
        for depth in range(8):
            parent = c_ulong()
            if not self.x11.XGetTransientForHint(self.display, window, byref(parent)):
                # Pinned rfd's GTK file-dialog constructor always passes a
                # null parent, even when Tauri supplies a Save parent. An
                # existing hint must still lead to the owned main window.
                return depth == 0
            if parent.value == self.main:
                return True
            if not parent.value or parent.value == window or self._pid(parent.value) != self.pid:
                return False
            window = parent.value
        return False

    def find(self) -> int | None:
        from ctypes import byref, c_char_p, c_void_p, cast
        from runtime import _x11_children
        matches = []
        counts = {"named": 0, "owned": 0, "valid_parent": 0, "viewable": 0}
        for window in _x11_children(self.x11, self.display):
            name = c_void_p()
            if not self.x11.XFetchName(self.display, window, byref(name)) or not name:
                continue
            try:
                named = cast(name, c_char_p).value == self.title
            finally:
                self.x11.XFree(name)
            if not named:
                continue
            counts["named"] += 1
            if self._pid(window) != self.pid:
                continue
            counts["owned"] += 1
            if not self._valid_parent(window):
                continue
            counts["valid_parent"] += 1
            attrs = self.Attributes()
            if self.x11.XGetWindowAttributes(self.display, window, byref(attrs)) and attrs.map_state == 2:
                counts["viewable"] += 1
                matches.append(window)
        self.discovery = counts
        assert len(matches) <= 1, "The native chooser must be the one owned modal window"
        return matches[0] if matches else None

    def _inside(self, window: int, ancestor: int) -> bool:
        from ctypes import POINTER, byref, c_uint, c_ulong
        for _ in range(16):
            if window == ancestor:
                return True
            if window in (0, 1):
                return False
            parent, root, count, children = c_ulong(), c_ulong(), c_uint(), POINTER(c_ulong)()
            try:
                if not self.x11.XQueryTree(self.display, window, byref(root), byref(parent), byref(children), byref(count)):
                    return False
                if parent.value == window or parent.value == root.value:
                    return False
                window = parent.value
            finally:
                if children:
                    self.x11.XFree(children)
        return False

    def _focus(self) -> int:
        from ctypes import byref, c_int, c_ulong
        window, revert = c_ulong(), c_int()
        self.x11.XGetInputFocus(self.display, byref(window), byref(revert))
        return window.value

    def activate(self) -> None:
        assert self.window and self.find() == self.window, "Only the owned native chooser may receive focus"
        # The isolated Xvfb session can leave focus on a GTK auxiliary window
        # after Open closes. Activate only the verified chooser, then require
        # focus to reach it before sending any key (and before every key).
        self.x11.XRaiseWindow(self.display, self.window)
        self.x11.XSetInputFocus(self.display, self.window, 2, 0)
        self.x11.XSync(self.display, 0)
        self.check_focus()

    def check_focus(self) -> None:
        assert self.window and self.find() == self.window and self._inside(self._focus(), self.window), \
            "The owned native chooser must retain keyboard focus"

    def _emit(self, code: int, pressed: bool) -> None:
        assert code and self.xtest.XTestFakeKeyEvent(self.display, code, int(pressed), 0), "Native chooser keyboard event failed"

    def _key(self, keysym: int) -> None:
        self.check_focus()
        code = self.x11.XKeysymToKeycode(self.display, keysym)
        assert code, "The selected X11 keyboard must support the fixture character"
        base = self.x11.XkbKeycodeToKeysym(self.display, code, 0, 0)
        shifted = self.x11.XkbKeycodeToKeysym(self.display, code, 0, 1)
        shift = self.x11.XKeysymToKeycode(self.display, 0xFFE1)
        needs_shift = base != keysym
        assert not needs_shift or shifted == keysym, "The fixture path must use the active ASCII keyboard layout"
        try:
            if needs_shift:
                self._emit(shift, True)
            self._emit(code, True)
            self._emit(code, False)
        finally:
            if needs_shift:
                self._emit(shift, False)
        self.x11.XSync(self.display, 0)

    def _control(self, letter: str) -> None:
        self.check_focus()
        control = self.x11.XKeysymToKeycode(self.display, 0xFFE3)
        try:
            self._emit(control, True)
            self._key(ord(letter))
        finally:
            self._emit(control, False)
            self.x11.XSync(self.display, 0)

    def enter_path(self, path: str) -> None:
        self._control("l")
        self._control("a")
        for character in path:
            self._key(ord(character))
        self._key(0xFF0D)


def choose_native_file(web, path: Path, *, save: bool = False) -> str:
    """Open/save one ASCII fixture through the real GTK chooser and its grants."""
    if os.name == "nt":
        raise NotImplementedError("The actual chooser probe uses Linux GTK/X11")
    from runtime import _x11_window

    path = path.resolve()
    text = str(path)
    assert text.isascii() and all(32 <= ord(character) < 127 for character in text), \
        "Native chooser fixtures require printable ASCII paths"
    assert path.parent.is_dir() and (not path.exists() if save else path.is_file()), \
        "Choose an existing input or a fresh save destination"
    title = SAVE_TITLE if save else OPEN_TITLE
    main, pid = _x11_window()
    chooser = _OwnedDialog(main, pid, title)
    try:
        web.script("""
          const state=window.__nativePermissionPicker={done:false};
          const options={title:arguments[1],defaultPath:arguments[2]};
          if(arguments[0]==='plugin:dialog|open')Object.assign(options,{multiple:false,directory:false});
          window.__TAURI_INTERNALS__.invoke(arguments[0],{options})
            .then(value=>Object.assign(state,{done:true,value}),()=>Object.assign(state,{done:true,error:true}));
        """, "plugin:dialog|save" if save else "plugin:dialog|open", title, str(path.parent))
        def discover():
            state = web.script("return window.__nativePermissionPicker;")
            assert not state.get("error"), "The native file chooser request was rejected"
            return chooser.find()

        try:
            chooser.window = wait_for(discover, bool, "the owned native file chooser appears", 15)
        except AssertionError:
            safe = {"save": save, **chooser.discovery}
            print("NATIVE CHOOSER OWNERSHIP: " + json.dumps(safe, sort_keys=True), flush=True)
            raise
        chooser.activate()
        chooser.enter_path(text)

        def result():
            return web.script("return window.__nativePermissionPicker;")

        # GTK Save can first resolve the location entry, then await its Save
        # action. Send at most one more Return, only to that same owned modal.
        deadline = time.monotonic() + 2
        state = result()
        while not state.get("done") and time.monotonic() < deadline:
            time.sleep(.05)
            state = result()
        if not state.get("done") and save and chooser.find() == chooser.window:
            chooser.check_focus()
            chooser._key(0xFF0D)
        state = wait_for(result, lambda state: state.get("done"), "the real file chooser returns its selection", 15)
        assert not state.get("error"), "The native file chooser returned an error"
        assert isinstance(state.get("value"), str) and Path(state["value"]).resolve() == path, \
            "The actual file chooser must select only the requested fixture"
        return state["value"]
    finally:
        chooser.close()
