module Paths_timsort (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName
  ) where

import qualified Control.Exception as Exception
import Data.Version (Version(..))
import System.Environment (getEnv)
catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
catchIO = Exception.catch


version :: Version
version = Version {versionBranch = [0,1], versionTags = []}
bindir, libdir, datadir, libexecdir :: FilePath

bindir     = "/home/tim/.cabal/bin"
libdir     = "/home/tim/.cabal/lib/timsort-0.1/ghc-7.4.1"
datadir    = "/home/tim/.cabal/share/timsort-0.1"
libexecdir = "/home/tim/.cabal/libexec"

getBinDir, getLibDir, getDataDir, getLibexecDir :: IO FilePath
getBinDir = catchIO (getEnv "timsort_bindir") (\_ -> return bindir)
getLibDir = catchIO (getEnv "timsort_libdir") (\_ -> return libdir)
getDataDir = catchIO (getEnv "timsort_datadir") (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "timsort_libexecdir") (\_ -> return libexecdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
