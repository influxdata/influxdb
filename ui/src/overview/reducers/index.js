import {combineReducers} from 'redux';
import time from './time';
import hosts from './hosts';

const rootReducer = combineReducers({
  hosts,
  time,
});

export default rootReducer;
