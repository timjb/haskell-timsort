{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE Rank2Types #-}

module Properties
  ( tests
  , timSortList
  ) where

import Test.Framework
import Test.QuickCheck
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Data.List (sort, sortBy)
import Data.Ord (comparing)
import Control.Monad.Primitive (PrimState)
import Control.Monad.ST (ST, runST)
import Data.Vector (MVector, fromList, toList, freeze, thaw)
import qualified Data.Vector.Algorithms.Tim as Tim
import Control.Applicative ((<$>))
import Control.Monad (forM)

testOptions :: TestOptions
testOptions = TestOptions
  { topt_seed = Nothing
  , topt_maximum_generated_tests = Just 10000
  , topt_maximum_unsuitable_generated_tests = Just 10000
  , topt_maximum_test_size = Nothing
  , topt_maximum_test_depth = Nothing
  , topt_timeout = Nothing
  }

tests :: Test
tests = plusTestOptions testOptions $ testGroup "timSort"
  [ testProperty "sorts like Data.List.sort" sortCorrect
  , testProperty "sorts lists with preexisting structure like Data.List.sort" sortCorrect'
  , testProperty "sorts like Data.List.sortBy" sortByCorrect
  , testProperty "gallopingSearchLeftPBounds is correct" $ testSearch Tim.gallopingSearchLeftPBounds
  , testProperty "gallopingSearchRightPBounds is correct" $ testSearch Tim.gallopingSearchRightPBounds
  , testProperty "minRun" computeMinRunCorrect
  ]

timSortList :: [Int] -> [Int]
timSortList xs = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.sort vec
  toList `fmap` freeze vec

newtype HalfSorted = HalfSorted [Int] deriving (Show, Eq)

instance Arbitrary HalfSorted where
  arbitrary = do
    runs <- choose (1,5)
    fmap (HalfSorted . concat) $ forM [1..(runs :: Int)] $ \_ -> do
      runLen <- choose (1,500)
      start <- choose (-10000,10000)
      step <- choose (-1000,1000)
      return $ map (\i -> start + i*step) [1..runLen]

sortCorrect :: [Int] -> Property
sortCorrect xs = sortBy cmp xs === timSortListBy cmp xs
  where cmp = comparing abs

sortCorrect' :: HalfSorted -> Property
sortCorrect' (HalfSorted xs) = sort xs === timSortList xs

timSortListBy :: (Int -> Int -> Ordering) -> [Int] -> [Int]
timSortListBy cmp xs = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.sortBy cmp vec
  toList `fmap` freeze vec

sortByCorrect :: [Int] -> Property
sortByCorrect xs = sortBy cmp xs === timSortListBy cmp xs
  where cmp = comparing abs

naiveSearch' :: (a -> Bool) -> [a] -> Int
naiveSearch' p = iter 0
  where
    iter !i [] = i
    iter !i (x:xs)
      | p x = i
      | otherwise = iter (i+1) xs

naiveSearch :: (a -> Bool) -> [a] -> Int -> Int -> Int
naiveSearch p xs l u
  | u <= l = l
  | otherwise = min u (l + naiveSearch' p (drop l xs))

data MonotonicListSlice =
  MonotonicListSlice { _listSliceList  :: [Bool]
                     , _listSliceLower :: Int
                     , _listSliceUpper :: Int
                     } deriving (Eq, Show)

instance Arbitrary MonotonicListSlice where
  arbitrary = do
    i <- abs <$> arbitrary
    j <- abs <$> arbitrary
    let list = replicate i False ++ replicate j True
        len = i + j
    l <- choose (0, max 0 (len-1))
    u <- choose (l, len)
    return (MonotonicListSlice list l u)

testSearch
  :: (forall s a. (a -> Bool) -> MVector (PrimState (ST s)) a -> Int -> Int -> ST s Int)
  -> MonotonicListSlice
  -> Property
testSearch search (MonotonicListSlice list l u) =
  let expected = naiveSearch id list l u
      actual =
        runST $ do
          vec <- thaw (fromList list)
          search id vec l u
  in expected === actual

powersOfTwo :: [Int]
powersOfTwo = iterate (*2) 1

nextPowerOfTwo :: Int -> Int
nextPowerOfTwo n = head (dropWhile (<n) powersOfTwo)

computeMinRunCorrect :: (Positive Int) -> Bool
computeMinRunCorrect (Positive n) = computeRuns (minRun - 1) > powerOf2
  where
    n' = n+1
    minRun = Tim.computeMinRun n'
    computeRuns i = if n' `mod` i == 0 then n' `div` i else (n' `div` i) + 1
    powerOf2 = nextPowerOfTwo (computeRuns minRun)