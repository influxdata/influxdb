properties([disableConcurrentBuilds()])

node("dind") {
    container('dind') {
        // This method is provided by the private api-compatibility library.
        compat.test_build()
    }
}
