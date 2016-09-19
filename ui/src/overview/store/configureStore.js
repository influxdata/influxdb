import {createStore, applyMiddleware} from 'redux';
import thunkMiddleware from 'redux-thunk';
import rootReducer from '../reducers';

import makeAppStorage from 'shared/middleware/appStorage';
import makeQueryExecuter from 'shared/middleware/queryExecuter';

export default function configureStore(effectiveWindow, initialState) {
  return createStore(
    rootReducer,
    initialState,
    applyMiddleware(
      thunkMiddleware,
      makeAppStorage(effectiveWindow.localStorage),
      makeQueryExecuter()
    )
  );
}
