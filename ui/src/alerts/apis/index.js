export function getAlerts() {
  const alerts = [
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
  return alerts;
}
