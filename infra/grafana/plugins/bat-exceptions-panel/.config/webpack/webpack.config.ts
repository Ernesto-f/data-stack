import CopyWebpackPlugin from 'copy-webpack-plugin';
import ForkTsCheckerWebpackPlugin from 'fork-ts-checker-webpack-plugin';
import LiveReloadPlugin from 'webpack-livereload-plugin';
import ReplaceInFileWebpackPlugin from 'replace-in-file-webpack-plugin';
import path from 'path';
import type { Configuration } from 'webpack';

import { getEntries, getPackageJson, getPluginJson, hasReadme, isWSL } from './utils';
import { DIST_DIR, SOURCE_DIR } from './constants';

const config = async (env: Record<string, unknown>): Promise<Configuration> => {
  const pluginJson = getPluginJson();
  const packageJson = getPackageJson();
  const isProduction = env.production;

  const entries = await getEntries();

  return {
    cache: {
      type: 'filesystem',
      buildDependencies: {
        config: [__filename],
      },
    },

    context: path.join(process.cwd(), SOURCE_DIR),

    devtool: isProduction ? 'source-map' : 'eval-source-map',

    entry: entries,

    externals: [
      'lodash',
      'jquery',
      'moment',
      'slate',
      'emotion',
      '@emotion/css',
      '@emotion/react',
      'prismjs',
      'slate-plain-serializer',
      '@grafana/slate-react',
      'react',
      'react-dom',
      'react-redux',
      'redux',
      'rxjs',
      'react-router',
      'react-router-dom',
      'd3',
      'angular',
      '@grafana/ui',
      '@grafana/runtime',
      '@grafana/data',
      function (context: any, request: any, callback: any) {
        const prefix = 'grafana/';
        if (request.indexOf(prefix) === 0) {
          return callback(null, request.slice(prefix.length));
        }
        callback();
      },
    ],

    mode: isProduction ? 'production' : 'development',

    module: {
      rules: [
        {
          exclude: /(node_modules)/,
          test: /\.[tj]sx?$/,
          use: {
            loader: 'swc-loader',
            options: {
              jsc: {
                baseUrl: path.resolve(process.cwd(), SOURCE_DIR),
                target: 'es2015',
                loose: false,
                parser: {
                  syntax: 'typescript',
                  tsx: true,
                  decorators: false,
                  dynamicImport: true,
                },
              },
            },
          },
        },
        {
          test: /\.css$/,
          use: ['style-loader', 'css-loader'],
        },
        {
          test: /\.s[ac]ss$/,
          use: ['style-loader', 'css-loader', 'sass-loader'],
        },
        {
          test: /\.(png|jpe?g|gif|webp)$/,
          type: 'asset/resource',
          generator: {
            publicPath: `public/plugins/${pluginJson.id}/`,
            outputPath: 'img/',
            filename: isProduction ? '[hash][ext]' : '[name][ext]',
          },
        },
        {
          test: /\.(woff|woff2|eot|ttf|otf)(\?v=\d+\.\d+\.\d+)?$/,
          type: 'asset/resource',
          generator: {
            publicPath: `public/plugins/${pluginJson.id}/`,
            outputPath: 'fonts/',
            filename: isProduction ? '[hash][ext]' : '[name][ext]',
          },
        },
        {
          test: /\.svg$/,
          type: 'asset/source',
        },
      ],
    },

    output: {
      clean: {
        keep: /gpx_*/,
      },
      filename: '[name].js',
      library: {
        type: 'amd',
      },
      path: path.resolve(process.cwd(), DIST_DIR),
      publicPath: '/',
    },

    plugins: [
      new CopyWebpackPlugin({
        patterns: [
          { from: hasReadme() ? 'README.md' : '../README.md', to: '.', force: true },
          { from: 'plugin.json', to: '.' },
          { from: '../LICENSE', to: '.', force: true, noErrorOnMissing: true },
          { from: '../CHANGELOG.md', to: '.', force: true, noErrorOnMissing: true },
          { from: '**/*.json', to: '.', globOptions: { ignore: ['**/tsconfig.json'] } },
          { from: '**/*.svg', to: '.', force: true, noErrorOnMissing: true },
          { from: '**/*.png', to: '.', force: true, noErrorOnMissing: true },
          { from: 'img/**/*', to: '.', force: true, noErrorOnMissing: true },
        ],
      }),
      new ReplaceInFileWebpackPlugin([
        {
          dir: DIST_DIR,
          files: ['plugin.json', 'README.md'],
          rules: [
            {
              search: '%VERSION%',
              replace: packageJson.version,
            },
            {
              search: '%TODAY%',
              replace: new Date().toISOString().substring(0, 10),
            },
            {
              search: '%PLUGIN_ID%',
              replace: pluginJson.id,
            },
          ],
        },
      ]),
      new ForkTsCheckerWebpackPlugin({
        async: Boolean(!isProduction),
        issue: {
          include: [{ file: '**/*.{ts,tsx}' }],
        },
        typescript: { configFile: path.join(process.cwd(), 'tsconfig.json') },
      }),
      ...(isProduction ? [] : [new LiveReloadPlugin()]),
    ],

    resolve: {
      extensions: ['.js', '.jsx', '.ts', '.tsx'],
      unsafeCache: true,
    },

    ...(isWSL() ? { watchOptions: { poll: 500 } } : {}),
  };
};

export default config;
