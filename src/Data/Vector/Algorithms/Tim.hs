{-# LANGUAGE BangPatterns #-}

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
import Control.Monad (liftM, when)
import Data.Bits ((.|.), (.&.), shiftL, shiftR)

type Comparison e = e -> e -> Ordering

sort :: (PrimMonad m, MVector v e, Ord e)
     => v (PrimState m) e -> m ()
sort = sortBy compare
{-# INLINABLE sort #-}

{-
data Run = Run { runStartIndex :: {-# UNPACK #-} !Int
               , runLength     :: {-# UNPACK #-} !Int
               } deriving (Eq, Show)
-}

sortBy :: (PrimMonad m, MVector v e)
       => Comparison e -> v (PrimState m) e -> m ()
sortBy cmp vec = iter [0] 0
  where
    len = length vec
    minRun = computeMinRun len
    --iter :: [Run] -> Int -> m ()
    iter s i | i >= len = performRemainingMerges s
    iter s i | otherwise = do
      (order, runLen) <- countRun cmp vec i len
      when (order == Descending) $ reverseSlice i runLen vec
      let runEnd = min len (i + max runLen minRun)
      sortByBounds' cmp vec i (i+runLen) runEnd
      --when (i /= 0) $ merge cmp vec 0 i runEnd
      s' <- performMerges (i : s) runEnd
      iter s' runEnd
    runLengthInvariantBroken a b c i = (b - a <= i - b) || (c - b <= i - c)
    performMerges (c:b:a:ss) i | runLengthInvariantBroken a b c i =
      if i - c <= b - a
        then merge cmp vec b c i >> performMerges (b:a:ss) i
        else merge cmp vec a b c >> performMerges (c:a:ss) i
    performMerges s _ = return s
    performRemainingMerges (b:a:ss) = do
      merge cmp vec a b len
      performRemainingMerges (a:ss)
    performRemainingMerges _ = return ()
{-# INLINE sortBy #-}

data Order = Ascending | Descending deriving (Eq, Show)

-- | Given `N`, this function calculates `minrun` in the range [32,65] s.t. in
--     q, r = divmod(N, minrun)
-- `q` is a power of 2 (or slightly less than a power of 2).
computeMinRun :: Int -> Int
computeMinRun = loop 0
  where
    -- Take the first 6 bits of `N` and add one if one of the remaining bits
    -- are set.
    loop !r n | n < 64 = r + n
    loop !r n = loop (r .|. (n .&. 1)) (n `shiftR` 1)
{-# INLINE computeMinRun #-}

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
      if key `lte` b then goLeft (i `shiftL` 1 + 1) i
                     else binarySearch (hint-i+1) (hint-j)

    goRight i j | hint + i >= len = do
      b <- unsafeRead vec (len-1)
      if key `gt` b then return len
                    else binarySearch (hint+j+1) (len-1)
    goRight i j = do
      b <- unsafeRead vec (hint+i)
      if key `gt` b then goRight (i `shiftL` 1 + 1) i
                    else binarySearch (hint+j+1) (hint+i)
{-# INLINE gallopLeft #-}

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
{-# INLINE gallopRight #-}

countRun :: (PrimMonad m, MVector v e)
         => Comparison e -> v (PrimState m) e -> Int -> Int -> m (Order, Int)
countRun _ _ i len | i+1 >= len = return (Ascending, 1)
countRun cmp vec i len = do
  x <- unsafeRead vec i
  y <- unsafeRead vec (i+1)
  if x `gt` y
    then descending y 2
    else ascending  y 2
  where
    gt  a b = cmp a b == GT
    lte a b = cmp a b /= GT

    descending _ !k | i + k >= len = return (Descending, k)
    descending x !k = do
      y <- unsafeRead vec (i+k)
      if x `gt` y then descending y (k+1)
                  else return (Descending, k)

    ascending _ !k | i + k >= len = return (Ascending, k)
    ascending x !k = do
      y <- unsafeRead vec (i+k)
      if x `lte` y then ascending y (k+1)
                   else return (Ascending, k)
{-# INLINE countRun #-}
 
reverseSlice :: (PrimMonad m, MVector v e)
             => Int -> Int -> v (PrimState m) e -> m ()
reverseSlice i len = reverse . slice i len
{-# INLINE reverseSlice #-}

cloneSlice :: (PrimMonad m, MVector v e)
           => Int -> Int -> v (PrimState m) e -> m (v (PrimState m) e)
cloneSlice i len = clone . slice i len
{-# INLINE cloneSlice #-}

mergeLo, mergeHi, merge :: (PrimMonad m, MVector v e)
                        => Comparison e -> v (PrimState m) e -> Int -> Int -> Int -> m ()

minGallop :: Int
minGallop = 7
{-# INLINE minGallop #-}

mergeLo cmp vec i j k = do
  cc <- cloneSlice i ccLen vec
  iter cc i 0 j minGallop minGallop
  where
    lte a b = cmp a b /= GT
    ccLen = j-i
    iter _  _ y _ _ _ | y >= ccLen = return ()
    iter cc x y z _ _ | z >= k = do
      let from = slice y (ccLen-y) cc
          to   = slice x (ccLen-y) vec
      unsafeCopy to from
    iter cc x y z 0 _ = do
      vz <- unsafeRead vec z
      gallopLen <- gallopRight cmp (slice y (ccLen-y) cc) vz 0 (ccLen-y)
      let from = slice y gallopLen cc
          to   = slice x gallopLen vec
      unsafeCopy to from
      iter cc (x+gallopLen) (y+gallopLen) z minGallop minGallop
    iter cc x y z _ 0 = do
      vy <- unsafeRead cc y
      gallopLen <- gallopLeft cmp (slice z (k-z) vec) vy 0 (k-z)
      let from = slice z gallopLen vec
          to   = slice x gallopLen vec
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
{-# INLINE mergeLo #-}

mergeHi cmp vec i j k = do
  cc <- cloneSlice j ccLen vec
  iter cc (k-1) (j-1) (ccLen-1) minGallop minGallop
  where
    gt a b = cmp a b == GT
    ccLen = k-j
    iter _  _ _ z _ _ | z < 0 = return ()
    iter cc _ y z _ _ | y < i = do
      let from = slice 0 (z+1) cc
          to   = slice i (z+1) vec
      unsafeCopy to from
    iter cc x y z 0 _ = do
      vz <- unsafeRead cc z
      gallopIndex <- gallopRight cmp (slice i (y-i) vec) vz (y-i-1) (y-i)
      let gallopLen = (y-i) - gallopIndex
          from = slice (y-gallopLen+1) gallopLen vec
          to   = slice (x-gallopLen+1) gallopLen vec
      unsafeMove to from
      iter cc (x-gallopLen) (y-gallopLen) z minGallop minGallop
    iter cc x y z _ 0 = do
      vy <- unsafeRead vec y
      gallopIndex <- gallopLeft cmp cc vy z (z+1)
      let gallopLen = (z+1) - gallopIndex
          from = slice (z-gallopLen+1) gallopLen cc
          to   = slice (x-gallopLen+1) gallopLen vec
      unsafeCopy to from
      iter cc (x-gallopLen) y (z-gallopLen) minGallop minGallop
    iter cc x y z ga gb = do
      vy <- unsafeRead vec y
      vz <- unsafeRead cc z
      if vy `gt` vz
        then do
          unsafeWrite vec x vy
          iter cc (x-1) (y-1) z (ga-1) minGallop
        else do
          unsafeWrite vec x vz
          iter cc (x-1) y (z-1) minGallop (gb-1)
{-# INLINE mergeHi #-}

merge cmp vec i j k = do
  b <- unsafeRead vec j
  i' <- (+i) `liftM` gallopRight cmp (slice i (j-i) vec) b 0 (j-i)
  when (i' < j) $ do
    a <- unsafeRead vec (j-1)
    k' <- (+j) `liftM` gallopLeft cmp (slice j (k-j) vec) a (k-j-1) (k-j)
    when (j < k) $
      (if (j-i) <= (k-j) then mergeLo else mergeHi) cmp vec i' j k'
{-# INLINE merge #-}
