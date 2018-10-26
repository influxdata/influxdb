import AJAX from 'src/utils/ajax'

export const logout = async (url): Promise<void> => {
  return AJAX({
    method: 'POST',
    url,
  })
}
