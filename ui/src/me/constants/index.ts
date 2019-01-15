import _ from 'lodash'

interface Greeting {
  text: string
  language: string
}

const randomGreetings: Greeting[] = [
  {
    text: 'Greetings',
    language: 'English',
  },
  {
    text: 'Ahoy',
    language: 'Pirate',
  },
  {
    text: 'Howdy',
    language: 'Texas',
  },
  {
    text: 'Bonjour',
    language: 'French',
  },
  {
    text: 'Hola',
    language: 'Spanish',
  },
  {
    text: 'Ciao',
    language: 'Italian',
  },
  {
    text: 'Hallo',
    language: 'German',
  },
  {
    text: 'Guten Tag',
    language: 'German',
  },
  {
    text: 'Olà',
    language: 'Portuguese',
  },
  {
    text: 'Namaste',
    language: 'Hindi',
  },
  {
    text: 'Salaam',
    language: 'Farsi',
  },
  {
    text: 'Ohayo',
    language: 'Japanese',
  },
  {
    text: 'こんにちは',
    language: 'Japanese',
  },
  {
    text: 'Merhaba',
    language: 'Turkish',
  },
  {
    text: 'Szia',
    language: 'Hungarian',
  },
  {
    text: 'Jambo',
    language: 'Swahili',
  },
  {
    text: '你好',
    language: 'Chinese (Simplified)',
  },
  {
    text: 'مرحبا',
    language: 'Arabic',
  },
  {
    text: 'Բարեւ',
    language: 'Armenian',
  },
  {
    text: 'Zdravo',
    language: 'Croatian',
  },
  {
    text: 'Привет',
    language: 'Russian',
  },
  {
    text: 'Xin chào',
    language: 'Vietnamese',
  },
  {
    text: 'สวัสดี',
    language: 'Thai',
  },
  {
    text: 'สวัสดี',
    language: 'Thai',
  },
  {
    text: 'Dzień dobry',
    language: 'Polish',
  },
  {
    text: 'Hei',
    language: 'Finnish',
  },
  {
    text: 'γεια σας',
    language: 'Greek',
  },
  {
    text: '인사말',
    language: 'Korean',
  },
  {
    text: 'Salve',
    language: 'Latin',
  },
  {
    text: 'Cyfarchion',
    language: 'Welsh',
  },
  {
    text: 'Ukubingelela',
    language: 'Zulu',
  },
  {
    text: 'Beannachtaí',
    language: 'Irish',
  },
  {
    text: '01001000 01100101 01101100 01101100 01101111',
    language: 'Binary',
  },
  {
    text: '.... . .-.. .-.. ---',
    language: 'Morse Code',
  },
  {
    text: 'nuqneH',
    language: 'Klingon',
  },
  {
    text: 'Saluton',
    language: 'Esperanto',
  },
]

export const generateRandomGreeting = (): Greeting => {
  return _.sample(randomGreetings)
}
