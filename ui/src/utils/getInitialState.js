export default function getInitialState(localStorage, hosts, alerts) {
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

  // Why are we getting hosts for all pages?
  // I've copied alerts here in the same way.
  return {
    hosts: h,
    alerts: alerts,
    time,
  };
}
