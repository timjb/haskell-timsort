module Data.Array.InsertionSort
  ( insertionSort
  ) where

import Data.Array
import Data.Array.MArray

insertionSort :: (MArray a e m, Ord e) => a Int e -> m (a Int e)
insertionSort arr = do
  (s, e) <- getBounds arr
  insert (s+1) s e
  return arr
  where
    swap i s | i <= s = return ()
    swap i s = do
      let j = i-1
      vj <- readArray arr j
      vi <- readArray arr i
      if vj <= vi then return () else do
        writeArray arr i vj
        writeArray arr j vi
        swap j s
    insert i _ e | i > e = return ()
    insert i s e = do
      swap i s
      insert (i+1) s e
