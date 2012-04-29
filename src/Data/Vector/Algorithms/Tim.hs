module Data.Vector.Algorithms.Tim
  ( sort
  , gallopLeft
  , gallopRight
  , computeMinRun
  ) where

import Prelude hiding (length, reverse)
import Data.Vector.Generic.Mutable
import Data.Vector.Algorithms.Search (binarySearchLByBounds, binarySearchRByBounds)
import Data.Vector.Algorithms.Insertion (sortByBounds')
import Control.Monad.Primitive (PrimMonad, PrimState)
import Control.Monad (when)
import Data.Function (fix)
import Data.Bits ((.|.), (.&.), shiftR)

sort :: (PrimMonad m, MVector v e, Ord e)
     => v (PrimState m) e -> m ()
sort vec = do
  let len = length vec
  let minRun = computeMinRun len
  let iter = fix $ \loop i -> if i >= len
        then return ()
        else do
          (order, runLen) <- countRun vec i len
          when (order == Descending) $ reverseSlice i runLen vec
          when (runLen < minRun) $ sortByBounds' compare vec i (i+runLen) (min len (i+minRun))
          when (i /= 0) $ merge vec 0 i (i+runLen)
          loop (i + runLen)
  iter 0

data Order = Ascending | Descending deriving (Eq, Show)

{-
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
-}

computeMinRun :: Int -> Int
computeMinRun = loop 0
  where
    loop r n | n < 64 = r + n
    loop r n = loop (r .|. (n .&. 1)) (n `shiftR` 1)

gallopLeft, gallopRight :: (PrimMonad m, MVector v e, Ord e)
                        => v (PrimState m) e -> e -> Int -> Int -> m Int

gallopLeft _ _ _ 0 = return 0
gallopLeft vec key hint len = do
  a <- unsafeRead vec hint
  if key <= a then goLeft  1 0
              else goRight 1 0
  where
    binarySearch = binarySearchLByBounds compare vec key
    goLeft i j | hint - i < 0 = do
      b <- unsafeRead vec 0
      if key <= b then return 0
                  else binarySearch 1 (hint-j)
    goLeft i j = do
      b <- unsafeRead vec (hint - i)
      if key <= b then goLeft (i*2 + 1) i
                  else binarySearch (hint-i+1) (hint-j)

    goRight i j | hint + i >= len = do
      b <- unsafeRead vec (len-1)
      if key > b then return len
                 else binarySearch (hint+j+1) (len-1)
    goRight i j = do
      b <- unsafeRead vec (hint+i)
      if key > b then goRight (i*2 + 1) i
                 else binarySearch (hint+j+1) (hint+i)

gallopRight _ _ _ 0 = return 0
gallopRight vec key hint len = do
  a <- unsafeRead vec hint
  if key < a then goLeft  1 0
             else goRight 1 0
  where
    binarySearch = binarySearchRByBounds compare vec key
    goLeft i j | hint - i < 0 = do
      b <- unsafeRead vec 0
      if key < b then return 0
                  else binarySearch 1 (hint-j)
    goLeft i j = do
      b <- unsafeRead vec (hint - i)
      if key < b then goLeft (i*2 + 1) i
                 else binarySearch (hint-i+1) (hint-j)

    goRight i j | hint + i >= len = do
      b <- unsafeRead vec (len-1)
      if key >= b then return len
                  else binarySearch (hint+j+1) (len-1)
    goRight i j = do
      b <- unsafeRead vec (hint+i)
      if key >= b then goRight (i*2 + 1) i
                  else binarySearch (hint+j+1) (hint+i)

countRun :: (PrimMonad m, MVector v e, Ord e)
         => v (PrimState m) e -> Int -> Int -> m (Order, Int)
countRun _ i len | i+1 >= len = return (Ascending, 1)
countRun vec i len = do
  x <- unsafeRead vec i
  y <- unsafeRead vec (i+1)
  if x > y
    then descending y (i+2) 2
    else ascending  y (i+2) 2
  where
    descending _ j k | j >= len = return (Descending, k)
    descending x j k = do
      y <- unsafeRead vec j
      if x > y then descending y (j+1) (k+1)
               else return (Descending, k)

    ascending _ j k | j >= len = return (Ascending, k)
    ascending x j k = do
      y <- unsafeRead vec j
      if x <= y then ascending y (j+1) (k+1)
                else return (Ascending, k)
 
reverseSlice :: (PrimMonad m, MVector v e)
             => Int -> Int -> v (PrimState m) e -> m ()
reverseSlice i len = reverse . slice i len

cloneSlice :: (PrimMonad m, MVector v e)
           => Int -> Int -> v (PrimState m) e -> m (v (PrimState m) e)
cloneSlice i len = clone . slice i len

merge :: (PrimMonad m, MVector v e, Ord e)
      => v (PrimState m) e -> Int -> Int -> Int -> m ()
merge vec i j k = do
  cc <- cloneSlice i ccLen vec
  iter cc i 0 j
  where
    ccLen = j-i
    iter _ _ y _ | y >= ccLen = return ()
    iter cc x y z | z >= k = do
      let from = slice y (ccLen-y) cc
      let to   = slice x (ccLen-y) vec
      unsafeCopy to from
    iter cc x y z = do
      vy <- unsafeRead cc y
      vz <- unsafeRead vec z
      if vy <= vz
        then do
          unsafeWrite vec x vy
          iter cc (x+1) (y+1) z
        else do
          unsafeWrite vec x vz
          iter cc (x+1) y (z+1)
