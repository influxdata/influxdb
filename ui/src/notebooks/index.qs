import React, {FC} from 'react'

const PIPES = {}

// NOTE: Dont get triggered by the MVC. every pipeline will have to control
// it's own state requirements, how it will render that state when required to
// and expose an interface to manipulate that state. So here we are.
export function register(type, model, view, controller) {
    if (PIPES.hasOwnProperty(type)) {
        throw new Exception(`Pipe of type [${type}] has already been registered`)
    }

    PIPES[type] = {
        view,
        controller
    }
}

const NotebookView: FC<Props> = ({
    type: number
}) => {

}

const DashboardContext: FC<Props> = ({
    view: string
}) => {
}

// NOTE: this loads in all the modules under the current directory
// to make it easier to add new types
const context = require.context("./", true, /index\.(js|jsx)$/);
context.keys().forEach((key) => {
    context(key);
});

