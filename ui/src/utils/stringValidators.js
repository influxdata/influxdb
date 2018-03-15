// uses Gajus' regex matcher from https://stackoverflow.com/questions/136505/searching-for-uuids-in-text-with-regex
export const isUUIDv4 = str =>
  str.length === 36 &&
  !!str.match(
    /[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}/
  )
