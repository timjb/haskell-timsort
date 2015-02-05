{-# LANGUAGE BangPatterns #-}

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
  , testProperty "sorts like Data.List.sortBy" sortByCorrect
  , testProperty "gallopLeft returns the first index i in a vector v where v[a] <= k for a key k" gallopLeftCorrect
  , testProperty "gallopRight returns the last index i in a vector v where v[a] <= k for a key k" gallopRightCorrect
  , testProperty "minRun" computeMinRunCorrect
  ]

timSortList :: [Int] -> [Int]
timSortList xs = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.sort vec
  toList `fmap` freeze vec

sortCorrect :: [Int] -> Bool
sortCorrect xs = sort xs == timSortList xs

timSortListBy :: (Int -> Int -> Ordering) -> [Int] -> [Int]
timSortListBy cmp xs = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.sortBy cmp vec
  toList `fmap` freeze vec

sortByCorrect :: [Int] -> Bool
sortByCorrect xs = sortBy cmp xs == timSortListBy cmp xs
  where cmp = comparing abs

gallopLeftList :: [Int] -> Int -> Int -> Int
gallopLeftList xs x hint = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.gallopLeft compare vec x hint (length xs)

gallopLeftCorrect :: OrderedList Int -> Int -> Gen Bool
gallopLeftCorrect (Ordered xs) x = do
  hint <- choose (0, length xs - 1)
  let naiveResult = length (takeWhile (<x) xs)
  return $ naiveResult == gallopLeftList xs x hint

gallopRightList :: [Int] -> Int -> Int -> Int
gallopRightList xs x hint = runST $ do
  vec <- thaw (fromList xs) :: ST s (MVector (PrimState (ST s)) Int)
  Tim.gallopRight compare vec x hint (length xs)

gallopRightCorrect :: OrderedList Int -> Int -> Gen Bool
gallopRightCorrect (Ordered xs) x = do
  hint <- choose (0, length xs - 1)
  let naiveResult = length (takeWhile (<=x) xs)
  return $ naiveResult == gallopRightList xs x hint

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