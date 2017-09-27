/* eslint-disable */
const CodeMirror = require('codemirror')

CodeMirror.defineSimpleMode = function(name, states) {
  CodeMirror.defineMode(name, function(config) {
    return CodeMirror.simpleMode(config, states)
  })
}

CodeMirror.simpleMode = function(config, states) {
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
          state:
            pers.state === state.localState
              ? s.localState
              : CodeMirror.copyState(pers.mode, pers.state),
          next: s.persistentStates,
        }
      }
      return s
    },
    token: tokenFunction(states_, config),
    innerMode(state) {
      return state.local && {mode: state.local.mode, state: state.localState}
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
  return function(stream, state) {
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
        } else if (rule.data.push) {
          ;(state.stack || (state.stack = [])).push(state.state)
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
              state.pending.push({text: matches[j], token: rule.token[j - 1]})
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
  const mode = pers
    ? pers.mode
    : spec.mode || CodeMirror.getMode(config, spec.spec)
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
  return function(state, textAfter, line) {
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
    {regex: /"(?:[^\\]|\\.)*?(?:"|$)/, token: 'string.double'},
    {regex: /'(?:[^\\]|\\.)*?(?:'|$)/, token: 'string.single'},
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
    {regex: /true|false|null|undefined|TRUE|FALSE/, token: 'atom'},
    {
      regex: /0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i,
      token: 'number',
    },
    {regex: /\/\/.*/, token: 'comment'},
    {regex: /\/(?:[^\\]|\\.)*?\//, token: 'variable-3'},
    // A next property will cause the mode to move to a different state
    {regex: /\/\*/, token: 'comment', next: 'comment'},
    {regex: /[-+\/*=<>!]+/, token: 'operator'},
    {regex: /[a-z$][\w$]*/, token: 'variable'},
  ],
  // The multi-line comment state.
  comment: [
    {regex: /.*?\*\//, token: 'comment', next: 'start'},
    {regex: /.*/, token: 'comment'},
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
