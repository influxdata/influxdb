import AJAX from 'src/utils/ajax'

export const writePoints = (source, db) => {
  return AJAX({
    url: source.links.write,
    method: 'POST',
    data: {},
  })
}
