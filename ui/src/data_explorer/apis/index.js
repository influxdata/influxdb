import AJAX from 'src/utils/ajax'

export const writeData = (source, db, data) => {
  return AJAX({
    url: `${source.links.write}?db=${db}`,
    method: 'POST',
    data,
  })
}
