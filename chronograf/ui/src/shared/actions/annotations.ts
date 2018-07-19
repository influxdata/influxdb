import * as api from 'src/shared/apis/annotation'
import {Dispatch} from 'redux'
import * as AnnotationsActions from 'src/types/actions/annotations'
import * as AnnotationsModels from 'src/types/annotations'

export const editingAnnotation = (): AnnotationsActions.EditingAnnotationAction => ({
  type: 'EDITING_ANNOTATION',
})

export const dismissEditingAnnotation = (): AnnotationsActions.DismissEditingAnnotationAction => ({
  type: 'DISMISS_EDITING_ANNOTATION',
})

export const addingAnnotation = (): AnnotationsActions.AddingAnnotationAction => ({
  type: 'ADDING_ANNOTATION',
})

export const addingAnnotationSuccess = (): AnnotationsActions.AddingAnnotationSuccessAction => ({
  type: 'ADDING_ANNOTATION_SUCCESS',
})

export const dismissAddingAnnotation = (): AnnotationsActions.DismissAddingAnnotationAction => ({
  type: 'DISMISS_ADDING_ANNOTATION',
})

export const mouseEnterTempAnnotation = (): AnnotationsActions.MouseEnterTempAnnotationAction => ({
  type: 'MOUSEENTER_TEMP_ANNOTATION',
})

export const mouseLeaveTempAnnotation = (): AnnotationsActions.MouseLeaveTempAnnotationAction => ({
  type: 'MOUSELEAVE_TEMP_ANNOTATION',
})

export const loadAnnotations = (
  annotations: AnnotationsModels.AnnotationInterface[]
): AnnotationsActions.LoadAnnotationsAction => ({
  type: 'LOAD_ANNOTATIONS',
  payload: {
    annotations,
  },
})

export const updateAnnotation = (
  annotation: AnnotationsModels.AnnotationInterface
): AnnotationsActions.UpdateAnnotationAction => ({
  type: 'UPDATE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const deleteAnnotation = (
  annotation: AnnotationsModels.AnnotationInterface
): AnnotationsActions.DeleteAnnotationAction => ({
  type: 'DELETE_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotation = (
  annotation: AnnotationsModels.AnnotationInterface
): AnnotationsActions.AddAnnotationAction => ({
  type: 'ADD_ANNOTATION',
  payload: {
    annotation,
  },
})

export const addAnnotationAsync = (
  createUrl: string,
  annotation: AnnotationsModels.AnnotationInterface
) => async dispatch => {
  dispatch(addAnnotation(annotation))
  const savedAnnotation = await api.createAnnotation(createUrl, annotation)
  dispatch(addAnnotation(savedAnnotation))
  dispatch(deleteAnnotation(annotation))
}

export const getAnnotationsAsync: AnnotationsActions.GetAnnotationsDispatcher = (
  indexUrl: string,
  {since, until}: AnnotationsModels.AnnotationRange
): AnnotationsActions.GetAnnotationsThunk => async (
  dispatch: Dispatch<AnnotationsActions.LoadAnnotationsAction>
): Promise<void> => {
  const annotations = await api.getAnnotations(indexUrl, since, until)
  dispatch(loadAnnotations(annotations))
}

export const deleteAnnotationAsync = (
  annotation: AnnotationsModels.AnnotationInterface
) => async dispatch => {
  await api.deleteAnnotation(annotation)
  dispatch(deleteAnnotation(annotation))
}

export const updateAnnotationAsync = (
  annotation: AnnotationsModels.AnnotationInterface
) => async dispatch => {
  await api.updateAnnotation(annotation)
  dispatch(updateAnnotation(annotation))
}
