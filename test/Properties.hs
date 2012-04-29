module Properties
  ( tests
  , timSortList
  ) where

import Test.Framework
import Test.QuickCheck
import Test.Framework.Providers.QuickCheck2 (testProperty)
import Data.List (sort)
import Control.Monad.ST (ST, runST)
import Data.Array.ST (STUArray)
import Data.Array.MArray (newListArray, getElems)
import Data.Array.TimSort (timSort)

tests :: Test
tests = testGroup "timSort"
  [ testProperty "sorts like Data.List.sort" sortCorrect
  ]

timSortList :: Int -> [Int] -> [Int]
timSortList offset list = runST $ do
  let end = offset + length list - 1
  arr <- newListArray (offset, end) list :: ST s (STUArray s Int Int)
  timSort arr
  getElems arr

sortCorrect :: (NonEmptyList Int) -> Int -> Bool
sortCorrect (NonEmpty list) offset = sort list == timSortList offset list