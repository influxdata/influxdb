properties([disableConcurrentBuilds()])

//13
node("dind") {
    container('dind') {
        compat.test_build()
    }
}
