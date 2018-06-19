import AJAX from 'src/utils/ajax'
import {AnnotationInterface} from 'src/types'

const msToRFCString = (ms: number) =>
  ms && new Date(Math.round(ms)).toISOString()

const rfcStringToMS = (rfc3339: string) => rfc3339 && Date.parse(rfc3339)

interface ServerAnnotation {
  id: string
  startTime: string
  endTime: string
  text: string
  type: string
  links: {self: string}
}

const annoToMillisecond = (
  annotation: ServerAnnotation
): AnnotationInterface => ({
  ...annotation,
  startTime: rfcStringToMS(annotation.startTime),
  endTime: rfcStringToMS(annotation.endTime),
})

const annoToRFC = (annotation: AnnotationInterface): ServerAnnotation => ({
  ...annotation,
  startTime: msToRFCString(annotation.startTime),
  endTime: msToRFCString(annotation.endTime),
})

export const createAnnotation = async (
  url: string,
  annotation: AnnotationInterface
) => {
  const data = annoToRFC(annotation)
  const response = await AJAX({method: 'POST', url, data})
  return annoToMillisecond(response.data)
}

export const getAnnotations = async (
  url: string,
  since: number,
  until: number
) => {
  const {data} = await AJAX({
    method: 'GET',
    url,
    params: {since: msToRFCString(since), until: msToRFCString(until)},
  })
  return data.annotations.map(annoToMillisecond)
}

export const deleteAnnotation = async (annotation: AnnotationInterface) => {
  const url = annotation.links.self
  await AJAX({method: 'DELETE', url})
}

export const updateAnnotation = async (annotation: AnnotationInterface) => {
  const url = annotation.links.self
  const data = annoToRFC(annotation)
  await AJAX({method: 'PATCH', url, data})
}
