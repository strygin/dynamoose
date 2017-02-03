'use strict';
module.exports = function(grunt) {
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    eslint: {
      src: ['gruntfile.js', 'index.js', 'lib/**/*.js'],
    },
    eslint2: {
      app: {
        src: ['gruntfile.js', 'index.js', 'lib/**/*.js'],
      },
      test: {
        src: ['test/**/*.js'],
      },
    },
    mochaTest: {
      test: {
        options: {
          reporter: 'spec',
        },
        src: ['test/**/*.js'],
      },
    },
    mocha_istanbul: {
      coverage: {
        src: 'test',
        options: {
          mask: '*.js',
        },
      },
    },
    clean: {
      coverage: {
        src: ['coverage/'],
      },
    },
  });

  // Load libs
  grunt.loadNpmTasks('gruntify-eslint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-clean');
  grunt.loadNpmTasks('grunt-mocha-istanbul');

  grunt.event.on('coverage', function(lcovFileContents, done) {
    done();
  });

  grunt.registerTask('default', ['eslint', 'mochaTest']);
  grunt.registerTask('test', ['eslint', 'mochaTest:test']);
  grunt.registerTask('test-nolint', ['mochaTest:test']);
  grunt.registerTask('coverage', ['eslint', 'clean:coverage', 'mocha_istanbul:coverage']);
};

