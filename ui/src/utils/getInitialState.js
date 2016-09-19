export default function getInitialState(localStorage, hosts) {
  const existingTimeSettingsString = localStorage.getItem('time');

  const h = {};
  hosts.forEach((host) => {
    h[host] = {url: host};
  });

  const time = Object.assign({
    bounds: {
      lower: 'now() - 15m',
      upper: 'now()',
    },
    groupByInterval: 60,
  }, JSON.parse(existingTimeSettingsString || '{}'));

  return {
    hosts: h,
    time,
  };
}
