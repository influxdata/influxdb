export const humanizeNote = (text: string): string => {
  return text.replace(/&gt;/g, '>').replace(/&#39;/g, "'")
}
