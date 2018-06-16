import * as api from 'src/shared/apis/annotation'
import {AnnotationInterface} from 'src/types'

export type Action =
  | EditingAnnotationAction
  | DismissEditingAnnotationAction
  | AddingAnnotationAction
  | AddingAnnotationSuccessAction
  | DismissAddingAnnotationAction
  | MouseEnterTempAnnotationAction
  | MouseLeaveTempAnnotationAction
  | LoadAnnotationsAction
  | UpdateAnnotationAction
  | DeleteAnnotationAction
  | AddAnnotationAction

export interface EditingAnnotationAction {
  type: 'EDITING_ANNOTATION'
}
export const editingAnnotation = (): EditingAnnotationAction => ({
  type: 'EDITING_ANNOTATION',
})

export interface DismissEditingAnnotationAction {
  type: 'DISMISS_EDITING_ANNOTATION'
}
export const dismissEditingAnnotation = (): DismissEditingAnnotationAction => ({
  type: 'DISMISS_EDITING_ANNOTATION',
})

export interface AddingAnnotationAction {
  type: 'ADDING_ANNOTATION'
}
export const addingAnnotation = (): AddingAnnotationAction => ({
  type: 'ADDING_ANNOTATION',
})

export interface AddingAnnotationSuccessAction {
  type: 'ADDING_ANNOTATION_SUCCESS'
}
export const addingAnnotationSuccess = (): AddingAnnotationSuccessAction => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
})

export interface DismissAddingAnnotationAction {
  type: 'DISMISS_ADDING_ANNOTATION'
}
export const dismissAddingAnnotation = (): DismissAddingAnnotationAction => ({
  type: 'DISMISS_ADDING_ANNOTATION',
})

export interface MouseEnterTempAnnotationAction {
  type: 'MOUSEENTER_TEMP_ANNOTATION'
}
export const mouseEnterTempAnnotation = (): MouseEnterTempAnnotationAction => ({
  type: 'MOUSEENTER_TEMP_ANNOTATION',
})

export interface MouseLeaveTempAnnotationAction {
  type: 'MOUSELEAVE_TEMP_ANNOTATION'
}
export const mouseLeaveTempAnnotation = (): MouseLeaveTempAnnotationAction => ({
  type: 'MOUSELEAVE_TEMP_ANNOTATION',
})

export interface LoadAnnotationsAction {
  type: 'LOAD_ANNOTATIONS'
  payload: {
    annotations: AnnotationInterface[]
  }
}
export const loadAnnotations = (
  annotations: AnnotationInterface[]
): LoadAnnotationsAction => ({
  type: 'LOAD_ANNOTATIONS',
  payload: {
    annotations,
  },
})

export interface UpdateAnnotationAction {
  type: 'UPDATE_ANNOTATION'
  payload: {
    annotation: AnnotationInterface
  }
}
export const updateAnnotation = (
  annotation: AnnotationInterface
): UpdateAnnotationAction => ({
  type: 'UPDATE_ANNOTATION',
  payload: {
    annotation,
  },
})

export interface DeleteAnnotationAction {
  type: 'DELETE_ANNOTATION'
  payload: {
    annotation: AnnotationInterface
  }
}
export const deleteAnnotation = (
  annotation: AnnotationInterface
): DeleteAnnotationAction => ({
  type: 'DELETE_ANNOTATION',
  payload: {
    annotation,
  },
})

export interface AddAnnotationAction {
  type: 'ADD_ANNOTATION'
  payload: {
    annotation: AnnotationInterface
  }
}
export const addAnnotation = (
  annotation: AnnotationInterface
): AddAnnotationAction => ({
  type: 'ADD_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotationAsync = (
  createUrl: string,
  annotation: AnnotationInterface
) => async dispatch => {
  dispatch(addAnnotation(annotation))
  const savedAnnotation = await api.createAnnotation(createUrl, annotation)
  dispatch(addAnnotation(savedAnnotation))
  dispatch(deleteAnnotation(annotation))
}

export interface AnnotationRange {
  since: number
  until: number
}

export const getAnnotationsAsync = (
  indexUrl: string,
  {since, until}: AnnotationRange
) => async dispatch => {
  const annotations = await api.getAnnotations(indexUrl, since, until)
  dispatch(loadAnnotations(annotations))
}

export const deleteAnnotationAsync = (
  annotation: AnnotationInterface
) => async dispatch => {
  await api.deleteAnnotation(annotation)
  dispatch(deleteAnnotation(annotation))
}

export const updateAnnotationAsync = (
  annotation: AnnotationInterface
) => async dispatch => {
  await api.updateAnnotation(annotation)
  dispatch(updateAnnotation(annotation))
}
