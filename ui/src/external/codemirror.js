/* eslint-disable */
const CodeMirror = require('codemirror')

CodeMirror.defineSimpleMode = function (name, states) {
  CodeMirror.defineMode(name, function (config) {
    return CodeMirror.simpleMode(config, states)
  })
}

CodeMirror.simpleMode = function (config, states) {
  ensureState(states, 'start')
  const states_ = {},
    meta = states.meta || {}
  let hasIndentation = false
  for (const state in states) {
    if (state !== meta && states.hasOwnProperty(state)) {
      const list = (states_[state] = []),
        orig = states[state]
      for (let i = 0; i < orig.length; i++) {
        const data = orig[i]
        list.push(new Rule(data, states))
        if (data.indent || data.dedent) {
          hasIndentation = true
        }
      }
    }
  }
  const mode = {
    startState() {
      return {
        state: 'start',
        pending: null,
        local: null,
        localState: null,
        indent: hasIndentation ? [] : null,
      }
    },
    copyState(state) {
      const s = {
        state: state.state,
        pending: state.pending,
        local: state.local,
        localState: null,
        indent: state.indent && state.indent.slice(0),
      }
      if (state.localState) {
        s.localState = CodeMirror.copyState(state.local.mode, state.localState)
      }
      if (state.stack) {
        s.stack = state.stack.slice(0)
      }
      for (let pers = state.persistentStates; pers; pers = pers.next) {
        s.persistentStates = {
          mode: pers.mode,
          spec: pers.spec,
          state: pers.state === state.localState ?
            s.localState : CodeMirror.copyState(pers.mode, pers.state),
          next: s.persistentStates,
        }
      }
      return s
    },
    token: tokenFunction(states_, config),
    innerMode(state) {
      return state.local && {
        mode: state.local.mode,
        state: state.localState
      }
    },
    indent: indentFunction(states_, meta),
  }
  if (meta) {
    for (const prop in meta) {
      if (meta.hasOwnProperty(prop)) {
        mode[prop] = meta[prop]
      }
    }
  }
  return mode
}

function ensureState(states, name) {
  if (!states.hasOwnProperty(name)) {
    throw new Error(`Undefined state ${name} in simple mode`)
  }
}

function toRegex(val, caret) {
  if (!val) {
    return /(?:)/
  }
  let flags = ''
  if (val instanceof RegExp) {
    if (val.ignoreCase) {
      flags = 'i'
    }
    val = val.source
  } else {
    val = String(val)
  }
  return new RegExp(`${caret === false ? '' : '^'}(?:${val})`, flags)
}

function asToken(val) {
  if (!val) {
    return null
  }
  if (val.apply) {
    return val
  }
  if (typeof val === 'string') {
    return val.replace(/\./g, ' ')
  }
  const result = []
  for (let i = 0; i < val.length; i++) {
    result.push(val[i] && val[i].replace(/\./g, ' '))
  }
  return result
}

function Rule(data, states) {
  if (data.next || data.push) {
    ensureState(states, data.next || data.push)
  }
  this.regex = toRegex(data.regex)
  this.token = asToken(data.token)
  this.data = data
}

function tokenFunction(states, config) {
  return function (stream, state) {
    if (state.pending) {
      const pend = state.pending.shift()
      if (state.pending.length === 0) {
        state.pending = null
      }
      stream.pos += pend.text.length
      return pend.token
    }

    if (state.local) {
      let tok, m
      if (state.local.end && stream.match(state.local.end)) {
        tok = state.local.endToken || null
        state.local = state.localState = null
        return tok
      }

      tok = state.local.mode.token(stream, state.localState)
      if (
        state.local.endScan &&
        (m = state.local.endScan.exec(stream.current()))
      ) {
        stream.pos = stream.start + m.index
      }
      return tok
    }

    const curState = states[state.state]
    for (let i = 0; i < curState.length; i++) {
      const rule = curState[i]
      const matches =
        (!rule.data.sol || stream.sol()) && stream.match(rule.regex)
      if (matches) {
        if (rule.data.next) {
          state.state = rule.data.next
        } else if (rule.data.push) {;
          (state.stack || (state.stack = [])).push(state.state)
          state.state = rule.data.push
        } else if (rule.data.pop && state.stack && state.stack.length) {
          state.state = state.stack.pop()
        }

        if (rule.data.mode) {
          enterLocalMode(config, state, rule.data.mode, rule.token)
        }
        if (rule.data.indent) {
          state.indent.push(stream.indentation() + config.indentUnit)
        }
        if (rule.data.dedent) {
          state.indent.pop()
        }
        let token = rule.token
        if (token && token.apply) {
          token = token(matches)
        }
        if (matches.length > 2) {
          state.pending = []
          for (let j = 2; j < matches.length; j++) {
            if (matches[j]) {
              state.pending.push({
                text: matches[j],
                token: rule.token[j - 1]
              })
            }
          }
          stream.backUp(
            matches[0].length - (matches[1] ? matches[1].length : 0)
          )
          return token[0]
        } else if (token && token.join) {
          return token[0]
        }
        return token
      }
    }
    stream.next()
    return null
  }
}

function cmp(a, b) {
  if (a === b) {
    return true
  }
  if (!a || typeof a !== 'object' || !b || typeof b !== 'object') {
    return false
  }
  let props = 0
  for (const prop in a) {
    if (a.hasOwnProperty(prop)) {
      if (!b.hasOwnProperty(prop) || !cmp(a[prop], b[prop])) {
        return false
      }
      props += 1
    }
  }
  for (const prop in b) {
    if (b.hasOwnProperty(prop)) {
      props -= 1
    }
  }
  return props === 0
}

function enterLocalMode(config, state, spec, token) {
  let pers
  if (spec.persistent) {
    for (let p = state.persistentStates; p && !pers; p = p.next) {
      if (spec.spec ? cmp(spec.spec, p.spec) : spec.mode === p.mode) {
        pers = p
      }
    }
  }
  const mode = pers ?
    pers.mode :
    spec.mode || CodeMirror.getMode(config, spec.spec)
  const lState = pers ? pers.state : CodeMirror.startState(mode)
  if (spec.persistent && !pers) {
    state.persistentStates = {
      mode,
      spec: spec.spec,
      state: lState,
      next: state.persistentStates,
    }
  }

  state.localState = lState
  state.local = {
    mode,
    end: spec.end && toRegex(spec.end),
    endScan: spec.end && spec.forceEnd !== false && toRegex(spec.end, false),
    endToken: token && token.join ? token[token.length - 1] : token,
  }
}

function indexOf(val, arr) {
  for (let i = 0; i < arr.length; i++) {
    if (arr[i] === val) {
      return true
    }
  }
}

function indentFunction(states, meta) {
  return function (state, textAfter, line) {
    if (state.local && state.local.mode.indent) {
      return state.local.mode.indent(state.localState, textAfter, line)
    }
    if (
      state.indent === null ||
      state.local ||
      (meta.dontIndentStates &&
        indexOf(state.state, meta.dontIndentStates) > -1)
    ) {
      return CodeMirror.Pass
    }

    let pos = state.indent.length - 1,
      rules = states[state.state]
    scan: for (;;) {
      for (let i = 0; i < rules.length; i++) {
        const rule = rules[i]
        if (rule.data.dedent && rule.data.dedentIfLineStart !== false) {
          const m = rule.regex.exec(textAfter)
          if (m && m[0]) {
            pos -= 1
            if (rule.next || rule.push) {
              rules = states[rule.next || rule.push]
            }
            textAfter = textAfter.slice(m[0].length)
            continue scan
          }
        }
      }
      break
    }
    return pos < 0 ? 0 : state.indent[pos]
  }
}

CodeMirror.defineSimpleMode('tickscript', {
  // The start state contains the rules that are intially used
  start: [
    // The regex matches the token, the token property contains the type
    {
      regex: /"(?:[^\\]|\\.)*?(?:"|$)/,
      token: 'string.double'
    },
    {
      regex: /'(?:[^\\]|\\.)*?(?:'|$)/,
      token: 'string.single'
    },
    {
      regex: /(function)(\s+)([a-z$][\w$]*)/,
      token: ['keyword', null, 'variable-2'],
    },
    // Rules are matched in the order in which they appear, so there is
    // no ambiguity between this one and the one above
    {
      regex: /(?:var|return|if|for|while|else|do|this|stream|batch|influxql|lambda)/,
      token: 'keyword',
    },
    {
      regex: /true|false|null|undefined|TRUE|FALSE/,
      token: 'atom'
    },
    {
      regex: /0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i,
      token: 'number',
    },
    {
      regex: /\/\/.*/,
      token: 'comment'
    },
    {
      regex: /\/(?:[^\\]|\\.)*?\//,
      token: 'variable-3'
    },
    // A next property will cause the mode to move to a different state
    {
      regex: /\/\*/,
      token: 'comment',
      next: 'comment'
    },
    {
      regex: /[-+\/*=<>!]+/,
      token: 'operator'
    },
    {
      regex: /[a-z$][\w$]*/,
      token: 'variable'
    },
  ],
  // The multi-line comment state.
  comment: [{
      regex: /.*?\*\//,
      token: 'comment',
      next: 'start'
    },
    {
      regex: /.*/,
      token: 'comment'
    },
  ],
  // The meta property contains global information about the mode. It
  // can contain properties like lineComment, which are supported by
  // all modes, and also directives like dontIndentStates, which are
  // specific to simple modes.
  meta: {
    dontIndentStates: ['comment'],
    lineComment: '//',
  },
})

// CodeMirror Hints

var HINT_ELEMENT_CLASS = "CodeMirror-hint";
var ACTIVE_HINT_ELEMENT_CLASS = "CodeMirror-hint-active";

// This is the old interface, kept around for now to stay backwards-compatible.
CodeMirror.showHint = function (cm, getHints, options) {
  if (!getHints) return cm.showHint(options);
  if (options && options.async) getHints.async = true;
  var newOpts = {
    hint: getHints
  };
  if (options)
    for (var prop in options) newOpts[prop] = options[prop];
  return cm.showHint(newOpts);
};

CodeMirror.defineExtension("showHint", function (options) {
  options = parseOptions(this, this.getCursor("start"), options);
  var selections = this.listSelections()
  if (selections.length > 1) return;
  // By default, don't allow completion when something is selected.
  // A hint function can have a `supportsSelection` property to
  // indicate that it can handle selections.
  if (this.somethingSelected()) {
    if (!options.hint.supportsSelection) return;
    // Don't try with cross-line selections
    for (var i = 0; i < selections.length; i++)
      if (selections[i].head.line != selections[i].anchor.line) return;
  }

  if (this.state.completionActive) this.state.completionActive.close();
  var completion = this.state.completionActive = new Completion(this, options);
  if (!completion.options.hint) return;

  CodeMirror.signal(this, "startCompletion", this);
  completion.update(true);
});

function Completion(cm, options) {
  this.cm = cm;
  this.options = options;
  this.widget = null;
  this.debounce = 0;
  this.tick = 0;
  this.startPos = this.cm.getCursor("start");
  this.startLen = this.cm.getLine(this.startPos.line).length - this.cm.getSelection().length;

  var self = this;
  cm.on("cursorActivity", this.activityFunc = function () {
    self.cursorActivity();
  });
}

var requestAnimationFrame = window.requestAnimationFrame || function (fn) {
  return setTimeout(fn, 1000 / 60);
};
var cancelAnimationFrame = window.cancelAnimationFrame || clearTimeout;

Completion.prototype = {
  close: function () {
    if (!this.active()) return;
    this.cm.state.completionActive = null;
    this.tick = null;
    this.cm.off("cursorActivity", this.activityFunc);

    if (this.widget && this.data) CodeMirror.signal(this.data, "close");
    if (this.widget) this.widget.close();
    CodeMirror.signal(this.cm, "endCompletion", this.cm);
  },

  active: function () {
    return this.cm.state.completionActive == this;
  },

  pick: function (data, i) {
    var completion = data.list[i];
    if (completion.hint) completion.hint(this.cm, data, completion);
    else this.cm.replaceRange(getText(completion), completion.from || data.from,
      completion.to || data.to, "complete");
    CodeMirror.signal(data, "pick", completion);
    this.close();
  },

  cursorActivity: function () {
    if (this.debounce) {
      cancelAnimationFrame(this.debounce);
      this.debounce = 0;
    }

    var pos = this.cm.getCursor(),
      line = this.cm.getLine(pos.line);
    if (pos.line != this.startPos.line || line.length - pos.ch != this.startLen - this.startPos.ch ||
      pos.ch < this.startPos.ch || this.cm.somethingSelected() ||
      (pos.ch && this.options.closeCharacters.test(line.charAt(pos.ch - 1)))) {
      this.close();
    } else {
      var self = this;
      this.debounce = requestAnimationFrame(function () {
        self.update();
      });
      if (this.widget) this.widget.disable();
    }
  },

  update: function (first) {
    if (this.tick == null) return
    var self = this,
      myTick = ++this.tick
    fetchHints(this.options.hint, this.cm, this.options, function (data) {
      if (self.tick == myTick) self.finishUpdate(data, first)
    })
  },

  finishUpdate: function (data, first) {
    if (this.data) CodeMirror.signal(this.data, "update");

    var picked = (this.widget && this.widget.picked) || (first && this.options.completeSingle);
    if (this.widget) this.widget.close();

    this.data = data;

    if (data && data.list.length) {
      if (picked && data.list.length == 1) {
        this.pick(data, 0);
      } else {
        this.widget = new Widget(this, data);
        CodeMirror.signal(data, "shown");
      }
    }
  }
};

function parseOptions(cm, pos, options) {
  var editor = cm.options.hintOptions;
  var out = {};
  for (var prop in defaultOptions) out[prop] = defaultOptions[prop];
  if (editor)
    for (var prop in editor)
      if (editor[prop] !== undefined) out[prop] = editor[prop];
  if (options)
    for (var prop in options)
      if (options[prop] !== undefined) out[prop] = options[prop];
  if (out.hint.resolve) out.hint = out.hint.resolve(cm, pos)
  return out;
}

function getText(completion) {
  if (typeof completion == "string") return completion;
  else return completion.text;
}

function buildKeyMap(completion, handle) {
  var baseMap = {
    Up: function () {
      handle.moveFocus(-1);
    },
    Down: function () {
      handle.moveFocus(1);
    },
    PageUp: function () {
      handle.moveFocus(-handle.menuSize() + 1, true);
    },
    PageDown: function () {
      handle.moveFocus(handle.menuSize() - 1, true);
    },
    Home: function () {
      handle.setFocus(0);
    },
    End: function () {
      handle.setFocus(handle.length - 1);
    },
    Enter: handle.pick,
    Tab: handle.pick,
    Esc: handle.close
  };
  var custom = completion.options.customKeys;
  var ourMap = custom ? {} : baseMap;

  function addBinding(key, val) {
    var bound;
    if (typeof val != "string")
      bound = function (cm) {
        return val(cm, handle);
      };
    // This mechanism is deprecated
    else if (baseMap.hasOwnProperty(val))
      bound = baseMap[val];
    else
      bound = val;
    ourMap[key] = bound;
  }
  if (custom)
    for (var key in custom)
      if (custom.hasOwnProperty(key))
        addBinding(key, custom[key]);
  var extra = completion.options.extraKeys;
  if (extra)
    for (var key in extra)
      if (extra.hasOwnProperty(key))
        addBinding(key, extra[key]);
  return ourMap;
}

function getHintElement(hintsElement, el) {
  while (el && el != hintsElement) {
    if (el.nodeName.toUpperCase() === "LI" && el.parentNode == hintsElement) return el;
    el = el.parentNode;
  }
}

function Widget(completion, data) {
  this.completion = completion;
  this.data = data;
  this.picked = false;
  var widget = this,
    cm = completion.cm;

  var hints = this.hints = document.createElement("ul");
  hints.className = "CodeMirror-hints";
  this.selectedHint = data.selectedHint || 0;

  var completions = data.list;
  for (var i = 0; i < completions.length; ++i) {
    var elt = hints.appendChild(document.createElement("li")),
      cur = completions[i];
    var className = HINT_ELEMENT_CLASS + (i != this.selectedHint ? "" : " " + ACTIVE_HINT_ELEMENT_CLASS);
    if (cur.className != null) className = cur.className + " " + className;
    elt.className = className;
    if (cur.render) cur.render(elt, data, cur);
    else elt.appendChild(document.createTextNode(cur.displayText || getText(cur)));
    elt.hintId = i;
  }

  var pos = cm.cursorCoords(completion.options.alignWithWord ? data.from : null);
  var left = pos.left,
    top = pos.bottom,
    below = true;
  hints.style.left = left + "px";
  hints.style.top = top + "px";
  // If we're at the edge of the screen, then we want the menu to appear on the left of the cursor.
  var winW = window.innerWidth || Math.max(document.body.offsetWidth, document.documentElement.offsetWidth);
  var winH = window.innerHeight || Math.max(document.body.offsetHeight, document.documentElement.offsetHeight);
  (completion.options.container || document.body).appendChild(hints);
  var box = hints.getBoundingClientRect(),
    overlapY = box.bottom - winH;
  var scrolls = hints.scrollHeight > hints.clientHeight + 1
  var startScroll = cm.getScrollInfo();

  if (overlapY > 0) {
    var height = box.bottom - box.top,
      curTop = pos.top - (pos.bottom - box.top);
    if (curTop - height > 0) { // Fits above cursor
      hints.style.top = (top = pos.top - height) + "px";
      below = false;
    } else if (height > winH) {
      hints.style.height = (winH - 5) + "px";
      hints.style.top = (top = pos.bottom - box.top) + "px";
      var cursor = cm.getCursor();
      if (data.from.ch != cursor.ch) {
        pos = cm.cursorCoords(cursor);
        hints.style.left = (left = pos.left) + "px";
        box = hints.getBoundingClientRect();
      }
    }
  }
  var overlapX = box.right - winW;
  if (overlapX > 0) {
    if (box.right - box.left > winW) {
      hints.style.width = (winW - 5) + "px";
      overlapX -= (box.right - box.left) - winW;
    }
    hints.style.left = (left = pos.left - overlapX) + "px";
  }
  if (scrolls)
    for (var node = hints.firstChild; node; node = node.nextSibling)
      node.style.paddingRight = cm.display.nativeBarWidth + "px"

  cm.addKeyMap(this.keyMap = buildKeyMap(completion, {
    moveFocus: function (n, avoidWrap) {
      widget.changeActive(widget.selectedHint + n, avoidWrap);
    },
    setFocus: function (n) {
      widget.changeActive(n);
    },
    menuSize: function () {
      return widget.screenAmount();
    },
    length: completions.length,
    close: function () {
      completion.close();
    },
    pick: function () {
      widget.pick();
    },
    data: data
  }));

  if (completion.options.closeOnUnfocus) {
    var closingOnBlur;
    cm.on("blur", this.onBlur = function () {
      closingOnBlur = setTimeout(function () {
        completion.close();
      }, 100);
    });
    cm.on("focus", this.onFocus = function () {
      clearTimeout(closingOnBlur);
    });
  }

  cm.on("scroll", this.onScroll = function () {
    var curScroll = cm.getScrollInfo(),
      editor = cm.getWrapperElement().getBoundingClientRect();
    var newTop = top + startScroll.top - curScroll.top;
    var point = newTop - (window.pageYOffset || (document.documentElement || document.body).scrollTop);
    if (!below) point += hints.offsetHeight;
    if (point <= editor.top || point >= editor.bottom) return completion.close();
    hints.style.top = newTop + "px";
    hints.style.left = (left + startScroll.left - curScroll.left) + "px";
  });

  CodeMirror.on(hints, "dblclick", function (e) {
    var t = getHintElement(hints, e.target || e.srcElement);
    if (t && t.hintId != null) {
      widget.changeActive(t.hintId);
      widget.pick();
    }
  });

  CodeMirror.on(hints, "click", function (e) {
    var t = getHintElement(hints, e.target || e.srcElement);
    if (t && t.hintId != null) {
      widget.changeActive(t.hintId);
      if (completion.options.completeOnSingleClick) widget.pick();
    }
  });

  CodeMirror.on(hints, "mousedown", function () {
    setTimeout(function () {
      cm.focus();
    }, 20);
  });

  CodeMirror.signal(data, "select", completions[this.selectedHint], hints.childNodes[this.selectedHint]);
  return true;
}

Widget.prototype = {
  close: function () {
    if (this.completion.widget != this) return;
    this.completion.widget = null;
    this.hints.parentNode.removeChild(this.hints);
    this.completion.cm.removeKeyMap(this.keyMap);

    var cm = this.completion.cm;
    if (this.completion.options.closeOnUnfocus) {
      cm.off("blur", this.onBlur);
      cm.off("focus", this.onFocus);
    }
    cm.off("scroll", this.onScroll);
  },

  disable: function () {
    this.completion.cm.removeKeyMap(this.keyMap);
    var widget = this;
    this.keyMap = {
      Enter: function () {
        widget.picked = true;
      }
    };
    this.completion.cm.addKeyMap(this.keyMap);
  },

  pick: function () {
    this.completion.pick(this.data, this.selectedHint);
  },

  changeActive: function (i, avoidWrap) {
    if (i >= this.data.list.length)
      i = avoidWrap ? this.data.list.length - 1 : 0;
    else if (i < 0)
      i = avoidWrap ? 0 : this.data.list.length - 1;
    if (this.selectedHint == i) return;
    var node = this.hints.childNodes[this.selectedHint];
    node.className = node.className.replace(" " + ACTIVE_HINT_ELEMENT_CLASS, "");
    node = this.hints.childNodes[this.selectedHint = i];
    node.className += " " + ACTIVE_HINT_ELEMENT_CLASS;
    if (node.offsetTop < this.hints.scrollTop)
      this.hints.scrollTop = node.offsetTop - 3;
    else if (node.offsetTop + node.offsetHeight > this.hints.scrollTop + this.hints.clientHeight)
      this.hints.scrollTop = node.offsetTop + node.offsetHeight - this.hints.clientHeight + 3;
    CodeMirror.signal(this.data, "select", this.data.list[this.selectedHint], node);
  },

  screenAmount: function () {
    return Math.floor(this.hints.clientHeight / this.hints.firstChild.offsetHeight) || 1;
  }
};

function applicableHelpers(cm, helpers) {
  if (!cm.somethingSelected()) return helpers
  var result = []
  for (var i = 0; i < helpers.length; i++)
    if (helpers[i].supportsSelection) result.push(helpers[i])
  return result
}

function fetchHints(hint, cm, options, callback) {
  if (hint.async) {
    hint(cm, callback, options)
  } else {
    var result = hint(cm, options)
    if (result && result.then) result.then(callback)
    else callback(result)
  }
}

function resolveAutoHints(cm, pos) {
  var helpers = cm.getHelpers(pos, "hint"),
    words
  if (helpers.length) {
    var resolved = function (cm, callback, options) {
      var app = applicableHelpers(cm, helpers);

      function run(i) {
        if (i == app.length) return callback(null)
        fetchHints(app[i], cm, options, function (result) {
          if (result && result.list.length > 0) callback(result)
          else run(i + 1)
        })
      }
      run(0)
    }
    resolved.async = true
    resolved.supportsSelection = true
    return resolved
  } else if (words = cm.getHelper(cm.getCursor(), "hintWords")) {
    return function (cm) {
      return CodeMirror.hint.fromList(cm, {
        words: words
      })
    }
  } else if (CodeMirror.hint.anyword) {
    return function (cm, options) {
      return CodeMirror.hint.anyword(cm, options)
    }
  } else {
    return function () {}
  }
}

CodeMirror.registerHelper("hint", "auto", {
  resolve: resolveAutoHints
});

CodeMirror.registerHelper("hint", "fromList", function (cm, options) {
  var cur = cm.getCursor(),
    token = cm.getTokenAt(cur)
  var term, from = CodeMirror.Pos(cur.line, token.start),
    to = cur
  if (token.start < cur.ch && /\w/.test(token.string.charAt(cur.ch - token.start - 1))) {
    term = token.string.substr(0, cur.ch - token.start)
  } else {
    term = ""
    from = cur
  }
  var found = [];
  for (var i = 0; i < options.words.length; i++) {
    var word = options.words[i];
    if (word.slice(0, term.length) == term)
      found.push(word);
  }

  if (found.length) return {
    list: found,
    from: from,
    to: to
  };
});

CodeMirror.commands.autocomplete = CodeMirror.showHint;

var defaultOptions = {
  hint: CodeMirror.hint.auto,
  completeSingle: true,
  alignWithWord: true,
  closeCharacters: /[\s()\[\]{};:>,]/,
  closeOnUnfocus: true,
  completeOnSingleClick: true,
  container: null,
  customKeys: null,
  extraKeys: null
};

CodeMirror.defineOption("hintOptions", null);
var WORD = /[\w$]+/,
  RANGE = 500;

CodeMirror.registerHelper("hint", "anyword", function (editor, options) {
  var word = options && options.word || WORD;
  var range = options && options.range || RANGE;
  var cur = editor.getCursor(),
    curLine = editor.getLine(cur.line);
  var end = cur.ch,
    start = end;
  while (start && word.test(curLine.charAt(start - 1))) --start;
  var curWord = start != end && curLine.slice(start, end);

  var list = options && options.list || [],
    seen = {};
  var re = new RegExp(word.source, "g");
  for (var dir = -1; dir <= 1; dir += 2) {
    var line = cur.line,
      endLine = Math.min(Math.max(line + dir * range, editor.firstLine()), editor.lastLine()) + dir;
    for (; line != endLine; line += dir) {
      var text = editor.getLine(line),
        m;
      while (m = re.exec(text)) {
        if (line == cur.line && m[0] === curWord) continue;
        if ((!curWord || m[0].lastIndexOf(curWord, 0) == 0) && !Object.prototype.hasOwnProperty.call(seen, m[0])) {
          seen[m[0]] = true;
          list.push(m[0]);
        }
      }
    }
  }
  return {
    list: list,
    from: CodeMirror.Pos(cur.line, start),
    to: CodeMirror.Pos(cur.line, end)
  };
});