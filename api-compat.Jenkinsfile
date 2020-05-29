properties([disableConcurrentBuilds()])

node("dind-1-12") {
    container('dind') {
        // This method is provided by the private api-compatibility library.
        compat.test_build()
    }
}
