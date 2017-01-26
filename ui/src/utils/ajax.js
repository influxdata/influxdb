import axios from 'axios';

export default function AJAX({
  url,
  method = 'GET',
  data = {},
  params = {},
  headers = {},
}) {
  if (window.basepath) {
    url = `${window.basepath}${url}`;
  }
  return axios({
    url,
    method,
    data,
    params,
    headers,
  });
}
