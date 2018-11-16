export const oneline = ({raw: [template]}: TemplateStringsArray) =>
  template.trim().replace(/\n(\s|\t)*/g, ' ')
