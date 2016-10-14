export default function getInitialState(localStorage, hosts, alerts) {
  const existingTimeSettingsString = localStorage.getItem('time');

  alerts = [
    {
      name: "High CPU",
      time: "12:00:01",
      value: "90%",
      host: "prod-influx-data-1",
      severity: "high",
    },
    {
      name: "Reavers",
      time: "12:00:02",
      value: "9000",
      host: "Firefly",
      severity: "ohgodohgod",
    },
  ];

  

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
