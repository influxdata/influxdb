const emptyFunny = [
  'Looks like you dont have any queries.  Be a lot cooler if you did.',
  'Create a query below. Go on, I dare ya!',
  'Create a query below.  Have fun!',
  '1) Create a query below \n2) Profit',
]

const getRandomInt = (min, max) =>
  Math.floor(Math.random() * (max - min + 1)) + min

export const emptyGraphCopy = emptyFunny[getRandomInt(0, 3)]
