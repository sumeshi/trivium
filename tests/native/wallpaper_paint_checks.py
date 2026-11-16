"""Compare actual WebView screenshots; no external image library is required."""
from __future__ import annotations

from feedback_checks import _settle
from row_navigation import wait_for


def geometry(web) -> dict:
    return web.script("""
      const surface=document.querySelector('.table-body-surface.has-wallpaper'),
        viewport=document.querySelector('.virtual-viewport');
      if(!surface||!viewport)return null;
      const r=surface.getBoundingClientRect(),v=viewport.getBoundingClientRect(),
        row=[...viewport.querySelectorAll('.data-row:not(.loading):not(.alt-row)')].find(e=>{
          const b=e.getBoundingClientRect();return b.top>=v.top&&b.bottom<=v.bottom;});
      if(!row)return null;
      const cells=[...row.querySelectorAll(':scope>.sticky')],
        annotations=cells.filter(e=>!e.hasAttribute('data-column-id')),
        right=items=>Math.min(v.right,Math.max(v.left,...items.map(e=>e.getBoundingClientRect().right))),
        base=surface.querySelector('.wallpaper-base'),mask=surface.querySelector('.wallpaper-pinned-mask'),
        alpha=color=>{const c=document.createElement('canvas');c.width=c.height=1;const ctx=c.getContext('2d');ctx.fillStyle=color;ctx.fillRect(0,0,1,1);return ctx.getImageData(0,0,1,1).data[3];},
        paint=e=>{const p=e.getBoundingClientRect(),s=getComputedStyle(e),image=getComputedStyle(e,'::before');
          return {rect:[p.left,p.top,p.width,p.height],opacity:s.opacity,backing:s.backgroundColor,
            backingAlpha:alpha(s.backgroundColor),clip:s.clipPath,imagePresent:image.backgroundImage!=='none',fit:image.backgroundSize,position:image.backgroundPosition,
            imageOpacity:image.opacity,pointer:s.pointerEvents};},
        outside=['.app-header','.project-tabs','.filter-primary','.table-header-scroll','.applied-filter-status'].map(selector=>{
          const e=document.querySelector(selector);if(!e)return null;const b=e.getBoundingClientRect();
          return {selector,containsWallpaper:!!e.querySelector('.wallpaper-layer'),
            overlap:Math.max(0,Math.min(r.right,b.right)-Math.max(r.left,b.left))*Math.max(0,Math.min(r.bottom,b.bottom)-Math.max(r.top,b.top))};
        }).filter(Boolean);
      const rowRect=row.getBoundingClientRect();
      return {rect:[r.left,r.top,r.width,r.height],rowRect:[rowRect.left,rowRect.top,rowRect.width,rowRect.height],annotationWidth:right(annotations)-r.left,
        pinnedWidth:right(cells)-r.left,base:base?paint(base):null,mask:mask?paint(mask):null,
        layers:surface.querySelectorAll('.wallpaper-layer').length,outside,
        sharedImage:!!base&&!!mask&&getComputedStyle(base,'::before').backgroundImage===getComputedStyle(mask,'::before').backgroundImage,
        rowOpacity:getComputedStyle(row).opacity,textOpacity:getComputedStyle(row.querySelector('.cell-text')).opacity};
    """)


def capture(web) -> str:
    _settle(web)
    return web.request("GET", web.route("screenshot"))


def changed_pixels(web, before: str, after: str, rect: list[float]) -> int:
    return web.async_script("""
      const done=arguments[arguments.length-1],sources=[arguments[0],arguments[1]],rect=arguments[2],
        viewport=[innerWidth,innerHeight];
      Promise.all(sources.map(source=>new Promise((resolve,reject)=>{
        const image=new Image();image.onload=()=>resolve(image);image.onerror=()=>reject(new Error('Screenshot decode failed'));
        image.src='data:image/png;base64,'+source;
      }))).then(images=>{
        const arrays=images.map(image=>{
          const sx=image.width/viewport[0],sy=image.height/viewport[1],canvas=document.createElement('canvas');
          canvas.width=image.width;canvas.height=image.height;const context=canvas.getContext('2d');context.drawImage(image,0,0);
          return context.getImageData(Math.round(rect[0]*sx),Math.round(rect[1]*sy),
            Math.max(1,Math.floor(rect[2]*sx)),Math.max(1,Math.floor(rect[3]*sy))).data;
        });
        if(arrays[0].length!==arrays[1].length){done({error:'Screenshot dimensions changed'});return;}
        let changed=0;
        for(let i=0;i<arrays[0].length;i+=4){
          if(Math.max(...[0,1,2].map(c=>Math.abs(arrays[0][i+c]-arrays[1][i+c])))>2)changed++;
        }
        done(changed);
      },error=>done({error:String(error)}));
    """, before, after, rect)


def assert_planes(web) -> dict:
    state = wait_for(lambda: geometry(web), lambda value: value and value["layers"] == 2,
                     "table body contains shared image and pinned-area mask")
    base, mask = state["base"], state["mask"]
    assert base["rect"] == mask["rect"] == state["rect"], state
    assert state["sharedImage"] and base["imagePresent"] and mask["imagePresent"], state
    assert base["fit"] == mask["fit"] == "cover" and base["position"] == mask["position"], state
    assert mask["opacity"] == "1" and mask["clip"] != "none", state
    assert mask["backingAlpha"] == 255, state
    assert base["pointer"] == mask["pointer"] == "none", state
    assert state["rowOpacity"] == state["textOpacity"] == "1", state
    assert all(not item["containsWallpaper"] and item["overlap"] == 0 for item in state["outside"]), state
    return state


def assert_no_text_leak(web, *, panned_frozen: bool = False) -> dict:
    state = assert_planes(web)
    x, y, width, height = state["rect"]
    protected = state["annotationWidth"] if panned_frozen else state["pinnedWidth"]
    assert protected > 20 and width - protected > 10, state
    rectangle = [x + 3, y + 3, protected - 6, height - 6]
    web.async_script("const done=arguments[arguments.length-1];setTimeout(()=>done(true),150);")
    before = capture(web)
    selector = '.data-row>.stickyData' if panned_frozen else '.data-row>[data-column-id]:not(.stickyData)'
    count = web.script("""
      const elements=[...document.querySelectorAll(arguments[0])];
      window.__wallpaperPaintRestore=elements.map(e=>({element:e,style:e.getAttribute('style')}));
      for(const e of elements){e.style.background='#ff00ff';e.style.color='#00ff00';}
      return elements.length;
    """, selector)
    try:
        assert count > 0
        after = capture(web)
        hidden = changed_pixels(web, before, after, rectangle)
        visible = changed_pixels(web, before, after, [x + protected + 3, y + 3, width - protected - 6, height - 6])
        assert hidden == 0, f"Hidden cell paint leaked through the fixed region: {hidden} pixels"
        assert isinstance(visible, int) and visible > 0, "The foreground mutation must visibly affect the exposed cells"
        return {"hidden_changed_pixels": hidden, "exposed_changed_pixels": visible,
                "kind": "frozen data under annotations" if panned_frozen else "scrolling data under fixed columns"}
    finally:
        web.script("""
          for(const item of window.__wallpaperPaintRestore??[]){
            if(item.style===null)item.element.removeAttribute('style');else item.element.setAttribute('style',item.style);
          }
          delete window.__wallpaperPaintRestore;
        """)
        _settle(web)
