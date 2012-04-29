module Data.Array.TimSort
  ( timSort
  ) where

import Data.Array
import Data.Array.MArray
import Data.Array.Base (unsafeRead)
import Control.Monad (when)
import Data.Function (fix)

timSort :: (MArray a e m, Ord e) => a Int e -> m ()
timSort arr = do
  (l, h) <- getBounds arr
  let sort = fix $ \loop i -> if i > h
        then return ()
        else do
          (order, len) <- countRun arr i h
          let runEnd = i+len-1
          when (order == Descending) $ reverseArray arr i runEnd
          when (i /= l) $ merge arr l i runEnd
          loop (runEnd+1)
  sort l

data Order = Ascending | Descending deriving (Eq, Show)

binaryInsertionPoint :: (Ord e, MArray a e m) => e -> a Int e -> Int -> Int -> m Int
binaryInsertionPoint _ _ m n | n >= m = return n
binaryInsertionPoint v arr m n = do
  let mid = (n+m) `div` 2
  midV <- readArray arr mid
  if v < midV
    then binaryInsertionPoint v arr m mid
    else binaryInsertionPoint v arr (mid+1) n

shiftRight :: (MArray a e m) => a Int e -> Int -> Int -> m ()
shiftRight _ m n | m > n = return ()
shiftRight arr m n = do
  v <- readArray arr n
  writeArray arr (n+1) v
  shiftRight arr m (n-1)

binaryInsertionSort :: (Ord e, MArray a e m) => a Int e -> Int -> Int -> Int -> m ()
binaryInsertionSort _ s m _ | s > m = return ()
binaryInsertionSort _ _ m n | m > n = return ()
binaryInsertionSort arr s m n = do
  v <- readArray arr m
  i <- binaryInsertionPoint v arr s (m-1)
  shiftRight arr i (m-1)
  writeArray arr i v
  binaryInsertionSort arr s (m+1) n

countRun :: (Ord e, MArray a e m) => a Int e -> Int -> Int -> m (Order, Int)
countRun _ m n | m >= n = return (Ascending, 1)
countRun arr m n = do
  first <- readArray arr m
  let m' = m+1
  second <- readArray arr m'
  if first > second
    then countRunDescending arr m' n 2
    else countRunAscending arr m' n 2

countRunDescending, countRunAscending :: (Ord e, MArray a e m) => a Int e -> Int -> Int -> Int -> m (Order, Int)
countRunDescending _ m n len | m >= n = return (Descending, len)
countRunDescending arr m n len = do
  current <- readArray arr m
  let m' = m+1
  next <- readArray arr m'
  if current > next
    then countRunDescending arr m' n (len+1)
    else return (Descending, len)

countRunAscending _ m n len | m >= n = return (Ascending, len)
countRunAscending arr m n len = do
  current <- readArray arr m
  let m' = m+1
  next <- readArray arr m'
  if current <= next
    then countRunAscending arr m' n (len+1)
    else return (Ascending, len)

freezePart :: (MArray a e m) => a Int e -> Int -> Int -> m (Array Int e)
freezePart _ m n | m > n = fail "lower bound must be lower than or equal to the upper bound"
freezePart arr m n = do
  (l,u) <- getBounds arr
  when (m < l) $ fail "lower bound is not in array"
  when (n > u) $ fail "upper bound is not in array"
  es <- mapM (unsafeRead arr) [(m-l) .. (n-l)]
  return (listArray (m,n) es)

merge :: (Ord e, MArray a e m) => a Int e -> Int -> Int -> Int -> m ()
merge arr s m n = do
  copy <- freezePart arr s (m-1)
  iter copy s s m
  where
    iter _ _ i _ | i >= m = return ()
    iter copy k i j | j > n = do
      writeArray arr k (copy ! i)
      iter copy (k+1) (i+1) j
    iter copy k i j = do
      let vi = copy ! i
      vj <- readArray arr j
      if vi <= vj
        then do
          writeArray arr k vi
          iter copy (k+1) (i+1) j
        else do
          writeArray arr k vj
          iter copy (k+1) i (j+1)

reverseArray :: (MArray a e m) => a Int e -> Int -> Int -> m ()
reverseArray _ m n | m >= n = return ()
reverseArray arr m n = do
  vm <- readArray arr m
  vn <- readArray arr n
  writeArray arr n vm
  writeArray arr m vn
  reverseArray arr (m+1) (n-1)