import update from 'react-addons-update';

export default function panels(state = {}, action) {
  switch (action.type) {
    case 'CREATE_PANEL': {
      const {panelId, queryId} = action.payload;
      const panel = {
        id: panelId,
        queryIds: [queryId],
      };

      return update(state, {
        [panelId]: {$set: panel},
      });
    }

    case 'RENAME_PANEL': {
      const {panelId, name} = action.payload;
      return update(state, {
        [panelId]: {
          name: {$set: name},
        },
      });
    }

    case 'DELETE_PANEL': {
      const {panelId} = action.payload;
      return update(state, {$apply: (p) => {
        const panelsCopy = Object.assign({}, p);
        delete panelsCopy[panelId];
        return panelsCopy;
      }});
    }

    case 'ADD_QUERY': {
      const {panelId, queryId} = action.payload;
      return update(state, {
        [panelId]: {
          queryIds: {$push: [queryId]},
        },
      });
    }

    case 'DELETE_QUERY': {
      const {panelId, queryId} = action.payload;
      return update(state, {
        [panelId]: {
          queryIds: {$set: state[panelId].queryIds.filter((id) => id !== queryId)},
        },
      });
    }
  }
  return state;
}
