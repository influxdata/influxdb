import u from 'updeep';

export default function hosts(state = {}, action) {
  switch (action.type) {
    case 'ADD_HOST': {
      const {url, params} = action.payload;

      const update = {
        [url]: {
          nickname: params.nickname,
          host: params.host,
          port: params.port,
          ssl: params.ssl,
          username: params.username,
          password: params.password,
        },
      };

      return u(update, state);
    }
    case 'LOAD_HOST_DIAGNOSTICS': {
      const allSeries = action.response.results[0].series;

      const networkSeries = allSeries.find((s) => s.name === 'network');
      const hostnameIndex = networkSeries.columns.indexOf('hostname');
      const hostname = networkSeries.values[0][hostnameIndex];

      const systemSeries = allSeries.find((s) => s.name === 'system');
      const uptimeIndex = systemSeries.columns.indexOf('uptime');
      const uptime = systemSeries.values[0][uptimeIndex];

      const buildSeries = allSeries.find((s) => s.name === 'build');
      const versionIndex = buildSeries.columns.indexOf('Version');
      const version = buildSeries.values[0][versionIndex];

      const update = {
        [action.url]: {
          diagnostics: {
            hostname,
            uptime,
            version,
          },
        },
      };

      return u(update, state);
    }
    case 'DELETE_HOST': {
      const stateCopy = Object.assign({}, state);
      delete stateCopy[action.payload.url];
      return stateCopy;
    }
    case 'LOAD_SERVERS_IN_CLUSTER': {
      const {host, dataNodes, metaNodes} = action.payload;

      const update = {
        [host]: {
          dataNodes,
          metaNodes,
        },
      };

      return u(update, state);
    }
    default:
      return state;
  }
}
