export const modeFlux = {
  // The start state contains the rules that are intially used
  start: [
    // Flux Syntax
    {
      regex: /[|][>]/,
      token: 'pipe-forward',
    },
    {
      regex: /\w+(?=[(])/,
      token: 'function',
    },
    {
      regex: /[\w\d]+(?=\s[=]\s)/,
      token: 'variable',
    },
    {
      regex: /[=][>]/,
      token: 'arrow-function',
    },
    {
      regex: /\w+(?=[)]\s[=][>])(?![(])/,
      token: 'function-arg',
    },
    {
      regex: /\w+(?=[[]["]|[.])/,
      token: 'function-arg-ref',
    },
    {
      regex: /AND|OR|[=][=]|[!][=]|[<][=]|[>][=]/,
      token: 'operator',
    },
    {
      regex: /\w+[:]/,
      token: 'argument',
    },
    // The regex matches the token, the token property contains the type
    {
      regex: /"(?:[^\\]|\\.)*?(?:"|$)/,
      token: 'string-double',
    },
    {
      regex: /'(?:[^\\]|\\.)*?(?:'|$)/,
      token: 'string-single',
    },
    {
      regex: /(function)(\s+)([a-z$][\w$]*)/,
      token: ['keyword', null, 'variable-2'],
    },
    {
      regex: /true|false|TRUE|FALSE/,
      token: 'boolean',
    },
    {
      regex: /null|undefined/,
      token: 'null',
    },
    {
      regex: /0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i,
      token: 'number',
    },
    {
      regex: /({.+:.+})/,
      token: 'object',
    },
    {
      regex: /\/\/.*/,
      token: 'comment',
    },
    // A next property will cause the mode to move to a different state
    {
      regex: /\/\*/,
      token: 'comment',
      next: 'comment',
    },
    {
      regex: /[-+\/*=<>!]+/,
      token: 'operator',
    },
  ],
  // The multi-line comment state.
  comment: [
    {
      regex: /.*?\*\//,
      token: 'comment',
      next: 'start',
    },
    {
      regex: /.*/,
      token: 'comment',
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
}

export const modeTickscript = {
  // The start state contains the rules that are intially used
  start: [
    // The regex matches the token, the token property contains the type
    {
      regex: /"(?:[^\\]|\\.)*?(?:"|$)/,
      token: 'string.double',
    },
    {
      regex: /'(?:[^\\]|\\.)*?(?:'|$)/,
      token: 'string.single',
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
      token: 'atom',
    },
    {
      regex: /0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i,
      token: 'number',
    },
    {
      regex: /\/\/.*/,
      token: 'comment',
    },
    {
      regex: /\/(?:[^\\]|\\.)*?\//,
      token: 'variable-3',
    },
    // A next property will cause the mode to move to a different state
    {
      regex: /\/\*/,
      token: 'comment',
      next: 'comment',
    },
    {
      regex: /[-+\/*=<>!]+/,
      token: 'operator',
    },
    {
      regex: /[a-z$][\w$]*/,
      token: 'variable',
    },
  ],
  // The multi-line comment state.
  comment: [
    {
      regex: /.*?\*\//,
      token: 'comment',
      next: 'start',
    },
    {
      regex: /.*/,
      token: 'comment',
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
}

export const modeInfluxQL = {
  // The start state contains the rules that are intially used
  start: [
    // The regex matches the token, the token property contains the type
    {
      regex: /"(?:[^\\]|\\.)*?(?:"|$)/,
      token: 'string.double',
    },
    {
      regex: /'(?:[^\\]|\\.)*?(?:'|$)/,
      token: 'string.single',
    },
    {
      regex: /(function)(\s+)([a-z$][\w$]*)/,
      token: ['keyword', null, 'variable-2'],
    },
    // Rules are matched in the order in which they appear, so there is
    // no ambiguity between this one and the one above
    {
      regex: /(SELECT\s|AS\s|FROM\s|WHERE\s|GROUP\sBY\s)/i,
      token: 'clause',
    },
    {
      regex: /FILL(?=[(])/i,
      token: 'clause',
    },
    {
      regex: /(CREATE\s|SHOW\s|DROP\s)/i,
      token: 'meta',
    },
    {
      regex: /0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i,
      token: 'number',
    },
    {
      regex: /[:](interval|dashboardTime|dashboardUpper)[:]/,
      token: 'temp-system',
    },
    {
      regex: /[:]\w+[:]/,
      token: 'temp-var',
    },
    {
      regex: /now[(][)]\s\S\s\S+/,
      token: 'now',
    },
    {
      regex: /[-+\/*=~<>!]+/,
      token: 'operator',
    },
    {
      regex: /(NULL)/i,
      token: 'null',
    },
  ],
  // The multi-line comment state.
  comment: [
    {
      regex: /.*?\*\//,
      token: 'comment',
      next: 'start',
    },
    {
      regex: /.*/,
      token: 'comment',
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
}

export const modeMarkdown = {
  start: [
    {
      regex: /[*](\s|\w)+[*]/,
      token: 'italic',
    },
    {
      regex: /[*][*](\s|\w)+[*][*]/,
      token: 'bold',
    },
    {
      regex: /[~][~](\s|\w)+[~][~]/,
      token: 'strikethrough',
    },
    {
      regex: /\#+\s.+(?=$)/gm,
      token: 'heading',
    },
    {
      regex: /\>.+(?=$)/gm,
      token: 'blockquote',
    },
    {
      regex: /\[.+\]\(.+\)/,
      token: 'link',
    },
    {
      regex: /[!]\[.+\]\(.+\)/,
      token: 'image',
    },
  ],
  comment: [
    {
      regex: /.*?\*\//,
      token: 'comment',
      next: 'start',
    },
    {
      regex: /.*/,
      token: 'comment',
    },
  ],
  meta: {
    dontIndentStates: ['comment'],
    lineComment: '//',
  },
}
