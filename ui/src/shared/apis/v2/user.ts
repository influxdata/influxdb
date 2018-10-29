import AJAX from 'src/utils/ajax'

interface User {
  name: string
  id: string
}

export const getMe = async (url: string): Promise<User> => {
  const {data} = await AJAX({
    url,
  })

  return data as User
}
