const alertaRegex = /(services)\('(.+?)'\)|(resource)\('(.+?)'\)|(event)\('(.+?)'\)|(environment)\('(.+?)'\)|(group)\('(.+?)'\)|(origin)\('(.+?)'\)|(token)\('(.+?)'\)/gi

export function parseAlerta(string) {
  const properties = []
  let match

  while ((match = alertaRegex.exec(string))) {
    // eslint-disable-line no-cond-assign
    for (let m = 1; m < match.length; m += 2) {
      if (match[m]) {
        properties.push({
          name: match[m],
          args:
            match[m] === 'services' ? match[m + 1].split(' ') : [match[m + 1]],
        })
      }
    }
  }

  return properties
}
