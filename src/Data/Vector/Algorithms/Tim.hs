module Data.Vector.Algorithms.Tim
  ( sort
  , sortBy
  , gallopLeft
  , gallopRight
  , computeMinRun
  ) where

import Prelude hiding (length, reverse)
import Data.Vector.Generic.Mutable
import Data.Vector.Algorithms.Search (binarySearchLByBounds, binarySearchRByBounds)
import Data.Vector.Algorithms.Insertion (sortByBounds')
import Control.Monad.Primitive (PrimMonad, PrimState)
import Control.Monad (when, liftM)
import Data.Function (fix)
import Data.Bits ((.|.), (.&.), shiftR)

type Comparison e = e -> e -> Ordering

sort :: (PrimMonad m, MVector v e, Ord e)
     => v (PrimState m) e -> m ()
sort = sortBy compare

sortBy :: (PrimMonad m, MVector v e)
       => Comparison e -> v (PrimState m) e -> m ()
sortBy cmp vec = do
  let len = length vec
  let minRun = computeMinRun len
  let iter = fix $ \loop i -> if i >= len
        then return ()
        else do
          (order, runLen) <- countRun cmp vec i len
          when (order == Descending) $ reverseSlice i runLen vec
          when (runLen < minRun) $ sortByBounds' cmp vec i (i+runLen) (min len (i+minRun))
          let runEnd = min len (i + max runLen minRun)
          when (i /= 0) $ merge cmp vec 0 i runEnd
          loop runEnd
  iter 0

data Order = Ascending | Descending deriving (Eq, Show)

computeMinRun :: Int -> Int
computeMinRun = loop 0
  where
    loop r n | n < 64 = r + n
    loop r n = loop (r .|. (n .&. 1)) (n `shiftR` 1)

gallopLeft, gallopRight :: (PrimMonad m, MVector v e)
                        => Comparison e -> v (PrimState m) e -> e -> Int -> Int -> m Int

gallopLeft _ _ _ _ 0 = return 0
gallopLeft cmp vec key hint len = do
  a <- unsafeRead vec hint
  if key `lte` a then goLeft  1 0
                 else goRight 1 0
  where
    gt  a b = cmp a b == GT
    lte a b = cmp a b /= GT
    binarySearch = binarySearchLByBounds cmp vec key

    goLeft i j | hint - i < 0 = do
      b <- unsafeRead vec 0
      if key `lte` b then return 0
                     else binarySearch 1 (hint-j)
    goLeft i j = do
      b <- unsafeRead vec (hint - i)
      if key `lte` b then goLeft (i*2 + 1) i
                     else binarySearch (hint-i+1) (hint-j)

    goRight i j | hint + i >= len = do
      b <- unsafeRead vec (len-1)
      if key `gt` b then return len
                    else binarySearch (hint+j+1) (len-1)
    goRight i j = do
      b <- unsafeRead vec (hint+i)
      if key `gt` b then goRight (i*2 + 1) i
                    else binarySearch (hint+j+1) (hint+i)

gallopRight _ _ _ _ 0 = return 0
gallopRight cmp vec key hint len = do
  a <- unsafeRead vec hint
  if key `lt` a then goLeft  1 0
                else goRight 1 0
  where
    lt  a b = cmp a b == LT
    gte a b = cmp a b /= LT
    binarySearch = binarySearchRByBounds cmp vec key

    goLeft i j | hint - i < 0 = do
      b <- unsafeRead vec 0
      if key `lt` b then return 0
                    else binarySearch 1 (hint-j)
    goLeft i j = do
      b <- unsafeRead vec (hint - i)
      if key `lt` b then goLeft (i*2 + 1) i
                    else binarySearch (hint-i+1) (hint-j)

    goRight i j | hint + i >= len = do
      b <- unsafeRead vec (len-1)
      if key `gte` b then return len
                     else binarySearch (hint+j+1) (len-1)
    goRight i j = do
      b <- unsafeRead vec (hint+i)
      if key `gte` b then goRight (i*2 + 1) i
                     else binarySearch (hint+j+1) (hint+i)

countRun :: (PrimMonad m, MVector v e)
         => Comparison e -> v (PrimState m) e -> Int -> Int -> m (Order, Int)
countRun _ _ i len | i+1 >= len = return (Ascending, 1)
countRun cmp vec i len = do
  x <- unsafeRead vec i
  y <- unsafeRead vec (i+1)
  if x `gt` y
    then descending y (i+2) 2
    else ascending  y (i+2) 2
  where
    gt  a b = cmp a b == GT
    lte a b = cmp a b /= GT

    descending _ j k | j >= len = return (Descending, k)
    descending x j k = do
      y <- unsafeRead vec j
      if x `gt` y then descending y (j+1) (k+1)
                  else return (Descending, k)

    ascending _ j k | j >= len = return (Ascending, k)
    ascending x j k = do
      y <- unsafeRead vec j
      if x `lte` y then ascending y (j+1) (k+1)
                   else return (Ascending, k)
 
reverseSlice :: (PrimMonad m, MVector v e)
             => Int -> Int -> v (PrimState m) e -> m ()
reverseSlice i len = reverse . slice i len

cloneSlice :: (PrimMonad m, MVector v e)
           => Int -> Int -> v (PrimState m) e -> m (v (PrimState m) e)
cloneSlice i len = clone . slice i len

mergeLo, merge :: (PrimMonad m, MVector v e)
               => Comparison e -> v (PrimState m) e -> Int -> Int -> Int -> m ()

data Side = Left | Right deriving (Eq, Show)

minGallop :: Int
minGallop = 7

mergeLo cmp vec i j k = do
  cc <- cloneSlice i ccLen vec
  iter cc i 0 j minGallop minGallop
  where
    lte a b = cmp a b /= GT
    ccLen = j-i
    iter _  _ y _ _ _ | y >= ccLen = return ()
    iter cc x y z _ _ | z >= k = do
      let from = slice y (ccLen-y) cc
      let to   = slice x (ccLen-y) vec
      unsafeCopy to from
    iter cc x y z 0 _ = do
      vz <- unsafeRead vec z
      gallopLen <- gallopRight cmp (slice y (ccLen-y) cc) vz 0 (ccLen-y)
      let from = slice y gallopLen cc
      let to   = slice x gallopLen vec
      unsafeCopy to from
      iter cc (x+gallopLen) (y+gallopLen) z minGallop minGallop
    iter cc x y z _ 0 = do
      vy <- unsafeRead cc y
      gallopLen <- gallopLeft cmp (slice z (k-z) vec) vy 0 (k-z)
      let from = slice z gallopLen vec
      let to   = slice x gallopLen vec
      unsafeCopy to from
      iter cc (x+gallopLen) y (z+gallopLen) minGallop minGallop
    iter cc x y z ga gb = do
      vy <- unsafeRead cc y
      vz <- unsafeRead vec z
      if vy `lte` vz
        then do
          unsafeWrite vec x vy
          iter cc (x+1) (y+1) z (ga-1) minGallop
        else do
          unsafeWrite vec x vz
          iter cc (x+1) y (z+1) minGallop (gb-1)

merge cmp vec i j k = do
  b <- unsafeRead vec j
  i' <- (+i) `liftM` gallopRight cmp (slice i (j-i) vec) b 0 (j-i)
  when (i' < j) $ do
    a <- unsafeRead vec (j-1)
    k' <- (+j) `liftM` gallopLeft cmp (slice j (k-j) vec) a (k-j-1) (k-j)
    when (j < k) $ do
      mergeLo cmp vec i' j k'