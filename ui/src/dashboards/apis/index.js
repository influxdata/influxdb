// import AJAX from 'utils/ajax';

export function getDashboards() {
  // return AJAX({
    // method: 'GET',
    // url: `/chronograf/v1/dashboards`,
  // });
  return {
    then(cb) {
      cb([
        {name: "Goldeneye System Status"},
        {name: "Carver Media Group Broadcast System Status"},
        {name: "Icarus Space Program Status"},
      ]);
    },
  };
}
