import u from 'updeep';

export const FETCHING = {};

export default function explorers(state = {}, action) {
  switch (action.type) {
    case 'FETCH_EXPLORERS': {
      return FETCHING;
    }

    case 'LOAD_EXPLORERS': {
      return action.payload.explorers.reduce((nextState, explorer) => {
        nextState[explorer.id] = {
          id: explorer.id,
          name: explorer.name,
          updatedAt: explorer.updated_at,
          createdAt: explorer.created_at,
        };
        return nextState;
      }, {});
    }

    case 'LOAD_EXPLORER': {
      const {explorer} = action.payload;

      const update = {
        [explorer.id]: normalizeExplorer(explorer),
      };

      return u(update, state);
    }

    case 'DELETE_EXPLORER': {
      const {id} = action.payload;

      return u(u.omit(id), state);
    }

    case 'EDIT_EXPLORER': {
      const {explorer} = action.payload;
      const update = {
        [explorer.id]: normalizeExplorer(explorer),
      };

      return u(update, state);
    }
  }

  return state;
}

function normalizeExplorer(explorer) {
  const {id, name, data, user_id, cluster_id, created_at, updated_at} = explorer;
  return Object.assign({}, explorer, {
    id,
    name,
    data,
    userID: user_id,
    clusterID: cluster_id,
    createdAt: created_at,
    updatedAt: updated_at,
  });
}
