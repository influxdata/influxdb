export interface AnnotationInterface {
  id: string
  startTime: number
  endTime: number
  text: string
  type: string
  links: {self: string}
}
