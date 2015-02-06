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
import Control.Monad (liftM, when)
import Data.Bits ((.|.), (.&.), shiftL, shiftR)

sort
  :: (PrimMonad m, MVector v e, Ord e)
  => v (PrimState m) e
  -> m ()
sort = sortBy compare
{-# INLINABLE sort #-}

newtype MergeState v m e = MergeState { _mergeVector :: v (PrimState m) e }

ensureMergeStateCapacity
  :: (PrimMonad m, MVector v e)
  => Int
  -> MergeState v m e
  -> m (MergeState v m e)
ensureMergeStateCapacity l (MergeState v) | l <= V.length v = return (MergeState v)
ensureMergeStateCapacity l _ = MergeState `liftM` V.new (2*l)

sortBy
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> m ()
sortBy cmp vec =
  if minRun == len
  then iter [0] 0 (error "no merge buffer needed!")
  else do
    mergeBuffer <- new 256
    iter [] 0 (MergeState mergeBuffer)
  where
    len = length vec
    minRun = computeMinRun len
    --iter :: [Run] -> Int -> m ()
    iter s i ms | i >= len = performRemainingMerges s ms
    iter s i ms | otherwise = do
      (order, runLen) <- countRun cmp vec i len
      when (order == Descending) $ reverse $ unsafeSlice i runLen vec
      let runEnd = min len (i + max runLen minRun)
      sortByBounds' cmp vec i (i+runLen) runEnd
      (s', ms') <- performMerges (i : s) runEnd ms
      iter s' runEnd ms'
    runLengthInvariantBroken a b c i = (b - a <= i - b) || (c - b <= i - c)
    performMerges [b,a] i ms | i - b >= b - a =
      merge cmp vec a b i ms >>= performMerges [a] i
    performMerges (c:b:a:ss) i ms | runLengthInvariantBroken a b c i =
      if i - c <= b - a
        then merge cmp vec b c i ms >>= performMerges (b:a:ss) i
        else merge cmp vec a b c ms >>= performMerges (c:a:ss) i
    performMerges s _ ms = return (s, ms)
    performRemainingMerges (b:a:ss) ms =
      merge cmp vec a b len ms >>= performRemainingMerges (a:ss)
    performRemainingMerges _ _ = return ()
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
      if p x
        then binSearch (j+1) i
        else iter (i+stepSize) i (2*stepSize)
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
      if p x
        then iter (i+stepSize) i (2*stepSize)
        else binSearch (i+1) j
{-# INLINE gallopingSearchRightPBounds #-}

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


cloneSlice
  :: (PrimMonad m, MVector v e)
  => Int
  -> Int
  -> v (PrimState m) e
  -> MergeState v m e
  -> m (MergeState v m e)
cloneSlice i len v ms = do
  MergeState cc <- ensureMergeStateCapacity len ms
  unsafeCopy (unsafeSlice 0 len cc) (unsafeSlice i len v)
  return (MergeState cc)
{-# INLINE cloneSlice #-}


minGallop :: Int
minGallop = 7
{-# INLINE minGallop #-}

-- | Merge the adjacent sorted slices [l,m) and [m,u). This is done by copying
-- the slice [l,m) to a temporary buffer.
mergeLo
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> MergeState v m e
  -> m (MergeState v m e)
mergeLo cmp vec l m u ms = do
  MergeState tmpBuf <- cloneSlice l tmpBufLen vec ms
  vi <- unsafeRead tmpBuf 0
  vj <- unsafeRead vec m
  iter tmpBuf 0 m l vi vj minGallop minGallop
  return (MergeState tmpBuf)
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

-- | Merge the adjacent sorted slices [l,m) and [m,u). This is done by copying
-- the slice [j,k) to a temporary buffer.
mergeHi
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> MergeState v m e
  -> m (MergeState v m e)
mergeHi cmp vec l m u ms = do
  MergeState tmpBuf <- cloneSlice m tmpBufLen vec ms
  vi <- unsafeRead vec (m-1)
  vj <- unsafeRead tmpBuf (tmpBufLen-1)
  iter tmpBuf (m-1) (tmpBufLen-1) (u-1) vi vj minGallop minGallop
  return (MergeState tmpBuf)
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

-- | Merge the adjacent sorted slices A=[l,m) and B=[m,u). This begins with
-- galloping searches to find the index of vec[m] in A and the index of vec[m-1]
-- in B to reduce the sizes of A and B. Then it uses `mergeHi` or `mergeLo`
-- depending on whether A or B is larger.
merge
  :: (PrimMonad m, MVector v e)
  => Comparison e
  -> v (PrimState m) e
  -> Int -- ^ l
  -> Int -- ^ m
  -> Int -- ^ u
  -> MergeState v m e
  -> m (MergeState v m e)
merge cmp vec l m u ms = do
  vm <- unsafeRead vec m
  l' <- gallopingSearchLeftPBounds (`gt` vm) vec l m
  if l' >= m
    then return ms
    else do
      vn <- unsafeRead vec (m-1)
      u' <- gallopingSearchRightPBounds (`gte` vn) vec m u
      if u' <= m
        then return ms
        else (if (m-l') <= (u'-m) then mergeLo else mergeHi) cmp vec l' m u' ms
  where
    gt  a b = cmp a b == GT
    gte a b = cmp a b /= LT
{-# INLINE merge #-}
