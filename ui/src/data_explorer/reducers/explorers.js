import u from 'updeep';

export const FETCHING = {};

export default function explorers(state = {}, action) {
  switch (action.type) {
    case 'FETCH_EXPLORERS': {
      return FETCHING;
    }

    case 'LOAD_EXPLORERS': {
      return action.payload.explorers.reduce((nextState, explorer) => {
        nextState[explorer.link.href] = normalizeExplorer(explorer);
        return nextState;
      }, {});
    }

    case 'LOAD_EXPLORER': {
      const {explorer} = action.payload;

      const update = {
        [explorer.link.href]: normalizeExplorer(explorer),
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
        [explorer.link.href]: normalizeExplorer(explorer),
      };

      return u(update, state);
    }
  }

  return state;
}

function normalizeExplorer(explorer) {
  const {link, name, data, user_id, created_at, updated_at} = explorer;
  return Object.assign({}, explorer, {
    id: link.href,
    name,
    data,
    userID: user_id,
    createdAt: created_at,
    updatedAt: updated_at,
  });
}
