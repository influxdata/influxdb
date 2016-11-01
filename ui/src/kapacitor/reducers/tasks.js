export default function tasks(state = {}, action) {
  switch (action.type) {
    case 'LOAD_TASK': {
      const {taskID, task} = action.payload;
      return Object.assign({}, state, {
        [taskID]: task,
      });
    }
  }
  return state;
}
