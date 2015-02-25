{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}

module Data.Vector.Algorithms.Tim
  ( sort
  , sortBy
  , gallopingSearchLeftPBounds
  , gallopingSearchRightPBounds
  , computeMinRun
  ) where

import Prelude hiding (length, reverse)
import Data.Vector.Generic.Mutable as V
import Data.Vector.Algorithms.Search (binarySearchPBounds)
import Data.Vector.Algorithms.Insertion (sortByBounds', Comparison)
import Control.Monad.Primitive (PrimMonad, PrimState)
import Control.Monad (when)
import Data.Bits ((.|.), (.&.), unsafeShiftR, popCount)

-- | Sorts an array using the default comparison.
sort
  :: (PrimMonad m, MVector v e, Ord e)
  => v (PrimState m) e
  -> m ()
sort = sortBy compare
{-# INLINABLE sort #-}

-- | Sorts an array using a custom comparison.
sortBy
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> m ()
sortBy cmp vec =
  if minRun == len
    then iter [0] 0 (error "no merge buffer needed!")
    else new 256 >>= iter [] 0
  where
    len = length vec
    minRun = computeMinRun len
    iter s i tmpBuf | i >= len = performRemainingMerges s tmpBuf
    iter s i tmpBuf | otherwise = do
      (order, runLen) <- nextRun cmp vec i len
      when (order == Descending) $ reverse $ unsafeSlice i runLen vec
      let runEnd = min len (i + max runLen minRun)
      sortByBounds' cmp vec i (i+runLen) runEnd
      (s', tmpBuf') <- performMerges (i : s) runEnd tmpBuf
      iter s' runEnd tmpBuf'
    runLengthInvariantBroken a b c i = (b - a <= i - b) || (c - b <= i - c)
    performMerges [b,a] i tmpBuf | i - b >= b - a =
      merge cmp vec a b i tmpBuf >>= performMerges [a] i
    performMerges (c:b:a:ss) i tmpBuf | runLengthInvariantBroken a b c i =
      if i - c <= b - a
        then merge cmp vec b c i tmpBuf >>= performMerges (b:a:ss) i
        else do
          tmpBuf' <- merge cmp vec a b c tmpBuf
          (ass', tmpBuf'') <- performMerges (a:ss) c tmpBuf'
          performMerges (c:ass') i tmpBuf''
    performMerges s _ tmpBuf = return (s, tmpBuf)
    performRemainingMerges (b:a:ss) tmpBuf =
      merge cmp vec a b len tmpBuf >>= performRemainingMerges (a:ss)
    performRemainingMerges _ _ = return ()
{-# INLINE sortBy #-}

-- | Given `N`, this function calculates `minRun` in the range [32,65] s.t. in
--     q, r = divmod(N, minRun)
-- `q` is a power of 2 (or slightly less than a power of 2).
computeMinRun :: Int -> Int
computeMinRun n0 = (n0 `unsafeShiftR` extra) + if (lowMask .&. n0) > 0 then 1 else 0
 where
   -- smear the bits down from the most significant bit
   !n1 = n0 .|. unsafeShiftR n0 1
   !n2 = n1 .|. unsafeShiftR n1 2
   !n3 = n2 .|. unsafeShiftR n2 4
   !n4 = n3 .|. unsafeShiftR n3 8
   !n5 = n4 .|. unsafeShiftR n4 16
   !n6 = n5 .|. unsafeShiftR n5 32
   -- mask for the bits lower than the 6 highest bits
   !lowMask = n6 `unsafeShiftR` 6
   !extra = popCount lowMask
{-# INLINE computeMinRun #-}

data Order = Ascending | Descending deriving (Eq, Show)

-- | Identify the next run (that is a monotonically increasing or strictly
-- decreasing sequence) in the slice [l,u) in vec. Returns the order and length
-- of the run.
nextRun
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ u
  -> m (Order, Int)
nextRun _ _ i len | i+1 >= len = return (Ascending, 1)
nextRun cmp vec i len = do
  x <- unsafeRead vec i
  y <- unsafeRead vec (i+1)
  if x `gt` y then desc y 2 else asc  y 2
  where
    gt a b = cmp a b == GT
    desc _ !k | i + k >= len = return (Descending, k)
    desc x !k = do
      y <- unsafeRead vec (i+k)
      if x `gt` y then desc y (k+1) else return (Descending, k)
    asc _ !k | i + k >= len = return (Ascending, k)
    asc x !k = do
      y <- unsafeRead vec (i+k)
      if x `gt` y then return (Ascending, k) else asc y (k+1)
{-# INLINE nextRun #-}

-- | Given a predicate that is guaranteed to be monotone on the indices [l,u) in
-- a given vector, finds the index in [l,u] at which the predicate turns from
-- False to True (yielding u if the entire interval is False).
-- Begins searching at l, going right in increasing (2^n)-steps.
gallopingSearchLeftPBounds
  :: (PrimMonad m, MVector v e)
  => (e -> Bool)
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ u
  -> m Int
gallopingSearchLeftPBounds p vec l u =
  if u <= l
    then return l
    else do
      x <- unsafeRead vec l
      if p x then return l else iter (l+1) l 2
  where
    binSearch = binarySearchPBounds p vec
    iter !i !j _stepSize | i >= u - 1 = do
      x <- unsafeRead vec (u-1)
      if p x then binSearch (j+1) (u-1) else return u
    iter !i !j !stepSize = do
      x <- unsafeRead vec i
      if p x then binSearch (j+1) i else iter (i+stepSize) i (2*stepSize)
{-# INLINE gallopingSearchLeftPBounds #-}

-- | Given a predicate that is guaranteed to be monotone on the indices [l,u) in
-- a given vector, finds the index in [l,u] at which the predicate turns from
-- False to True (yielding u if the entire interval is False).
-- Begins searching at u, going left in increasing (2^n)-steps.
gallopingSearchRightPBounds
  :: (PrimMonad m, MVector v e)
  => (e -> Bool)
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ u
  -> m Int
gallopingSearchRightPBounds p vec l u =
  if u <= l
    then return l
    else iter (u-1) (u-1) (-1)
  where
    binSearch = binarySearchPBounds p vec
    iter !i !j _stepSize | i <= l = do
      x <- unsafeRead vec l
      if p x then return l else binSearch (l+1) j
    iter !i !j !stepSize = do
      x <- unsafeRead vec i
      if p x then iter (i+stepSize) i (2*stepSize) else binSearch (i+1) j
{-# INLINE gallopingSearchRightPBounds #-}

-- | Tests if a temporary buffer has a given size. If not, allocates a new
-- buffer and returns it instead of the old temporary buffer.
ensureCapacity
  :: (PrimMonad m, MVector v e)
  => Int
  -> v (PrimState m) e
  -> m (v (PrimState m) e)
ensureCapacity l tmpBuf | l <= V.length tmpBuf = return tmpBuf
ensureCapacity l _tmpBuf = V.new (2*l)
{-# INLINE ensureCapacity #-}

-- | Copy the slice [i,i+len) from vec to tmpBuf. If tmpBuf is not large enough,
-- a new buffer is allocated and used. Returns the buffer.
cloneSlice
  :: (PrimMonad m, MVector v e)
  => Int -- ^ i
  -> Int -- ^ len
  -> v (PrimState m) e -- ^ vec
  -> v (PrimState m) e -- ^ tmpBuf
  -> m (v (PrimState m) e)
cloneSlice i len vec tmpBuf = do
  tmpBuf' <- ensureCapacity len tmpBuf
  unsafeCopy (unsafeSlice 0 len tmpBuf') (unsafeSlice i len vec)
  return tmpBuf'
{-# INLINE cloneSlice #-}

-- | Number of consecutive times merge chooses the element from the same run
-- before galloping mode is activated.
minGallop :: Int
minGallop = 7
{-# INLINE minGallop #-}

-- | Merge the adjacent sorted slices [l,m) and [m,u) in vec. This is done by
-- copying the slice [l,m) to a temporary buffer. Returns the (enlarged)
-- temporary buffer.
mergeLo
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e -- ^ vec
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> v (PrimState m) e -- ^ tmpBuf
  -> m (v (PrimState m) e)
mergeLo cmp vec l m u tempBuf' = do
  tmpBuf <- cloneSlice l tmpBufLen vec tempBuf'
  vi <- unsafeRead tmpBuf 0
  vj <- unsafeRead vec m
  iter tmpBuf 0 m l vi vj minGallop minGallop
  return tmpBuf
  where
    gt  a b = cmp a b == GT
    gte a b = cmp a b /= LT
    tmpBufLen = m - l
    iter _ i _ _ _ _ _ _ | i >= tmpBufLen = return ()
    iter tmpBuf i j k _ _ _ _ | j >= u = do
      let from = unsafeSlice i (tmpBufLen-i) tmpBuf
          to   = unsafeSlice k (tmpBufLen-i) vec
      unsafeCopy to from
    iter tmpBuf i j k _ vj 0 _ = do
      i' <- gallopingSearchLeftPBounds (`gt` vj) tmpBuf i tmpBufLen
      let gallopLen = i' - i
          from = unsafeSlice i gallopLen tmpBuf
          to   = unsafeSlice k gallopLen vec
      unsafeCopy to from
      vi' <- unsafeRead tmpBuf i'
      iter tmpBuf i' j (k+gallopLen) vi' vj minGallop minGallop
    iter tmpBuf i j k vi _ _ 0 = do
      j' <- gallopingSearchLeftPBounds (`gte` vi) vec j u
      let gallopLen = j' - j
          from = slice j gallopLen vec
          to   = slice k gallopLen vec
      unsafeMove to from
      vj' <- unsafeRead vec j'
      iter tmpBuf i j' (k+gallopLen) vi vj' minGallop minGallop
    iter tmpBuf i j k vi vj ga gb = do
      if vj `gte` vi
        then do
          unsafeWrite vec k vi
          vi' <- unsafeRead tmpBuf (i+1)
          iter tmpBuf (i+1) j (k+1) vi' vj (ga-1) minGallop
        else do
          unsafeWrite vec k vj
          vj' <- unsafeRead vec (j+1)
          iter tmpBuf i (j+1) (k+1) vi vj' minGallop (gb-1)
{-# INLINE mergeLo #-}

-- | Merge the adjacent sorted slices [l,m) and [m,u) in vec. This is done by
-- copying the slice [j,k) to a temporary buffer. Returns the (enlarged)
-- temporary buffer.
mergeHi
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e -- ^ vec
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> v (PrimState m) e -- ^ tmpBuf
  -> m (v (PrimState m) e)
mergeHi cmp vec l m u tmpBuf' = do
  tmpBuf <- cloneSlice m tmpBufLen vec tmpBuf'
  vi <- unsafeRead vec (m-1)
  vj <- unsafeRead tmpBuf (tmpBufLen-1)
  iter tmpBuf (m-1) (tmpBufLen-1) (u-1) vi vj minGallop minGallop
  return tmpBuf
  where
    gt  a b = cmp a b == GT
    gte a b = cmp a b /= LT
    tmpBufLen = u - m
    iter _ _ j _ _ _ _ _ | j < 0 = return ()
    iter tmpBuf i j _ _ _ _ _ | i < l = do
      let from = unsafeSlice 0 (j+1) tmpBuf
          to   = unsafeSlice l (j+1) vec
      unsafeCopy to from
    iter tmpBuf i j k _ vj 0 _ = do
      i' <- gallopingSearchRightPBounds (`gt` vj) vec l i
      let gallopLen = i - i'
          from = slice (i'+1) gallopLen vec
          to   = slice (k-gallopLen+1) gallopLen vec
      unsafeMove to from
      vi' <- unsafeRead vec i'
      iter tmpBuf i' j (k-gallopLen) vi' vj minGallop minGallop
    iter tmpBuf i j k vi _ _ 0 = do
      j' <- gallopingSearchRightPBounds (`gte` vi) tmpBuf 0 j
      let gallopLen = j - j'
          from = slice (j'+1) gallopLen tmpBuf
          to   = slice (k-gallopLen+1) gallopLen vec
      unsafeCopy to from
      vj' <- unsafeRead tmpBuf j'
      iter tmpBuf i j' (k-gallopLen) vi vj' minGallop minGallop
    iter tmpBuf i j k vi vj ga gb = do
      if vi `gt` vj
        then do
          unsafeWrite vec k vi
          vi' <- unsafeRead vec (i-1)
          iter tmpBuf (i-1) j (k-1) vi' vj (ga-1) minGallop
        else do
          unsafeWrite vec k vj
          vj' <- unsafeRead tmpBuf (j-1)
          iter tmpBuf i (j-1) (k-1) vi vj' minGallop (gb-1)
{-# INLINE mergeHi #-}

-- | Merge the adjacent sorted slices A=[l,m) and B=[m,u) in vec. This begins
-- with galloping searches to find the index of vec[m] in A and the index of
-- vec[m-1] in B to reduce the sizes of A and B. Then it uses `mergeHi` or
-- `mergeLo` depending on whether A or B is larger. Returns the (enlarged)
-- temporary buffer.
merge
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e -- ^ vec
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> v (PrimState m) e -- ^ tmpBuf
  -> m (v (PrimState m) e)
merge cmp vec l m u tmpBuf = do
  vm <- unsafeRead vec m
  l' <- gallopingSearchLeftPBounds (`gt` vm) vec l m
  if l' >= m
    then return tmpBuf
    else do
      vn <- unsafeRead vec (m-1)
      u' <- gallopingSearchRightPBounds (`gte` vn) vec m u
      if u' <= m
        then return tmpBuf
        else (if (m-l') <= (u'-m) then mergeLo else mergeHi) cmp vec l' m u' tmpBuf
  where
    gt  a b = cmp a b == GT
    gte a b = cmp a b /= LT
{-# INLINE merge #-}
