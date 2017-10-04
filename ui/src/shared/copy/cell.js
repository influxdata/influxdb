import _ from 'lodash'

const emptyFunny = [
  'Looks like you don’t have any queries. Be a lot cooler if you did.',
  'Create a query. Go on, I dare ya!',
  'Create a query. Have fun!',
  '1) Create a query \n2) Call your mom',
]

export const emptyGraphCopy = _.sample(emptyFunny)
