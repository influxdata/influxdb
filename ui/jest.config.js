module.exports = {
  projects: [
    {
      displayName: 'test',
      testPathIgnorePatterns: ['/build/'],
      modulePaths: ['<rootDir>', '<rootDir>/node_modules/'],
      moduleDirectories: ['src'],
      setupFiles: ['<rootDir>/test/setupTests.js'],
    },
    {
      runner: 'jest-runner-eslint',
      displayName: 'lint',
      testMatch: ['<rootDir>/test/**/*.test.js'],
    },
  ],
}
