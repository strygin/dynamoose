'use strict';
module.exports = function(grunt) {
  grunt.initConfig({
    pkg: grunt.file.readJSON('package.json'),
    jshint: {
      app: {
        src: ['gruntfile.js', 'index.js', 'lib/**/*.js'],
        options: {
          node: true,
          jshintrc: '.jshintrc'
        }
      },
      test: {
        src: ['test/**/*.js'],
        options: {
          node: true,
          jshintrc: 'test/.jshintrc',
          ignores: ['test/coverage/**/*.js']
        }
      }
    },
    mochaTest: {
      test: {
        options: {
          reporter: 'spec'
        },
        src: ['test/**/*.js']
      }
    },
    mocha_istanbul: {
      coverage: {
        src: 'test',
        options: {
          mask: '*.js'
        }
      }
    },
    clean: {
      coverage: {
        src: ['coverage/']
      }
    }
  });

  // Load libs
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-mocha-test');
  grunt.loadNpmTasks('grunt-contrib-clean');
  //grunt.loadNpmTasks('grunt-istanbul');
  grunt.loadNpmTasks('grunt-mocha-istanbul');

  grunt.event.on('coverage', function(lcovFileContents, done) {
    done();
  });

  grunt.registerTask('default', ['jshint', 'mochaTest']);
  grunt.registerTask('test', ['jshint', 'mochaTest:test']);
  grunt.registerTask('coverage', ['jshint', 'clean:coverage', 'mocha_istanbul:coverage']);

};

