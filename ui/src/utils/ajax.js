import axios from 'axios';

export default function AJAX({
  url,
  method = 'GET',
  data = {},
  params = {},
  headers = {},
}) {
  return axios({
    url,
    method,
    data,
    params,
    headers,
  });
}
